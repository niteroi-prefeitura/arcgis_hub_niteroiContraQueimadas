import os
import time
import json
import sched
import requests
import pandas as pd
from arcgis.gis import GIS
from arcgis.gis import Item
from dotenv import load_dotenv
from datetime import datetime, date, timedelta
from arcgis.features import FeatureLayerCollection
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from arcgis.geometry import Point, Polyline, Polygon, Geometry

load_dotenv()

AGOL_USERNAME: os.getenv("AGOL_USERNAME")
AGOL_PASSWORD: os.getenv("AGOL_PASSWORD")
LAYER_ID_AGOL_OCORRENCIAS_FOGO: os.getenv("LAYER_ID_AGOL_OCORRENCIAS_FOGO")
URL_GIS_ENTERPRISE_GEONITEROI: os.getenv("URL_GIS_ENTERPRISE_GEONITEROI")

URL_FOGO_API: os.getenv("URL_FOGO_API")


def update_df():
    """
    This function fetches data from the API, creates a DataFrame, and updates it with new records.
    """
    response = requests.get(URL_FOGO_API)
    var_dict = response.__dict__
    content = var_dict['_content']
    obj1 = json.loads(content)
    df0 = pd.DataFrame(obj1)
    df1 = pd.concat([df0.drop(['fields'], axis=1),
                    df0['fields'].apply(pd.Series)], axis=1)

    # Reordenar as colunas, colocando 'data' e 'hora' nas primeiras posições e mantendo o restante
    new_order = ['pk', 'data', 'bairro', 'log', 'incidente', 'location']

    # Aplicar a nova ordem ao DataFrame
    df = df1[new_order]
    df.loc[df['incidente'].isin([1, 2])]

    # Função para padronizar o formato da "location"

    def padronizar_location(location):
        if ',' in location and location.count(',') == 1:
            return location
        elif '/' in location and location.count('/') == 1:
            return location.replace(' / ', ',')
        else:
            return None

    # Aplicar a função para padronizar todas as linhas
    df['location'] = df['location'].apply(padronizar_location)

    # Remover linhas com valores inválidos (None)
    df = df.dropna(subset=['location'])

    # Separar a coluna 'location' em duas colunas: 'latitude' e 'longitude'
    df[['latitude', 'longitude']] = df['location'].str.split(',', expand=True)

    # Converter os valores para numérico
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    # Remover a coluna 'location' original se necessário
    df = df.drop(columns=['location'])
    df = df.drop(columns=['incidente'])

    return df


def execute():
    df = update_df()
    # Criar uma lista para armazenar os objetos GeoJSON
    geojson_features = []

    # Iterar sobre as linhas do DataFrame
    for index, row in df.iterrows():
        # Criar um dicionário para cada feature
        feature = {
            "type": "Feature",
            "geometry": {
                "x": row['longitude'],
                "y": row['latitude']
            },
            "attributes": {
                "pk": row['pk'],
                "data": row['data'],
                "bairro": row['bairro'],
                "logradouro": str(row['log']),
            }
        }
        # Adicionar a feature à lista
        geojson_features.append(feature)

    # GeoJSON final
    geojson_data = {
        "type": "FeatureCollection",
        "features": geojson_features
    }

    for feature in geojson_data['features']:
        # Verifique se o campo 'data' existe
        if 'data' in feature['attributes']:
            # Pega a string de data
            data_str = feature['attributes']['data']

            # Converter a string para datetime com o formato 'DD/MM/YYYY HH:MM:SS'
            try:
                dt = pd.to_datetime(data_str, format='%Y-%m-%dT%H:%M:%SZ')
                # Adicionar 3 horas
                dt += timedelta(hours=3)
                # Converter para timestamp em milissegundos
                timestamp = int(dt.timestamp() * 1000)
                feature['attributes']['data'] = timestamp

            except ValueError:
                # Atribuir None caso a conversão falhe
                feature['attributes']['data'] = None

    import numpy as np
    # Conectando ao Agol
    url = URL_GIS_ENTERPRISE_GEONITEROI
    usr = AGOL_USERNAME
    pwd = AGOL_PASSWORD
    gis = GIS(url, usr, pwd, verify_cert=False)
    layer_id = LAYER_ID_AGOL_OCORRENCIAS_FOGO

    gis = GIS(url, usr, pwd)

    # Localize a Feature Layer onde os dados estão armazenados (insira o seu Item ID)
    item = gis.content.get(layer_id)
    # Assumindo que você está trabalhando com a primeira camada
    layer = item.layers[0]

    # # Assumindo que a variável 'geojson_data' já contém o GeoJSON no formato correto
    # features = geojson_features
    features = geojson_data['features']

    layer_features = layer.query(
        where='1=1', return_geometry=False, as_df=True)
    layer_features = layer_features.fillna(999)
    features_to_delete = []
    features_to_add = []

    # Processar cada feature do GeoJSON
    for feature in features:

        pk_api = feature['attributes']['pk']  # 'pk' deve ser a chave primária

        existing_feature = layer_features.loc[layer_features['pk'] == pk_api]

        if existing_feature.empty:
            # Se não existe um registro com o mesmo 'pk', adicionar o novo
            features_to_add.append(feature)
        else:
            for column in existing_feature.columns:
                current_index = existing_feature[column].index.values[0]

                if column == 'data':
                    current_layer_value = int(
                        existing_feature[column][current_index].timestamp()) * 1000

                elif column == 'bairro':
                    current_layer_value = int(
                        existing_feature[column][current_index])

                else:
                    current_layer_value = existing_feature[column][current_index]

                try:
                    if column != 'logradouro' and np.isnan(feature['attributes'][column]):
                        current_api_value = 999
                    else:
                        current_api_value = feature['attributes'][column]

                    if current_layer_value != current_api_value:
                        features_to_add.append(feature)
                        features_to_delete.append(
                            existing_feature['OBJECTID'][current_index])
                        break

                except:
                    continue

    # Inserir novos registros ou registros modificados
    if features_to_add:
        append_result = layer.edit_features(adds=features_to_add)
        print(f"Append Result: {append_result}")
    else:
        print('Nenhuma feição adicionada')

    # Excluir registros antigos de mesmo 'pk'
    if features_to_delete:
        delete_result = layer.delete_features(deletes=str(features_to_delete))
        print(f"Delete Result: {delete_result}")
    else:
        print('Nenhuma feição deletada')


def task(scheduler):
    scheduler.enter(3600, 1, task, (scheduler,))
    print('Atualizado', datetime.now())
    update_df()


my_scheduler = sched.scheduler(time.time, time.sleep)
my_scheduler.enter(3600, 1, task, (my_scheduler,))
my_scheduler.run()

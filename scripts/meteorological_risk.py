import os
import time
import json
import sched
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, date, timedelta

from arcgis.gis import GIS
from arcgis.gis import Item

load_dotenv()


AGOL_USERNAME: os.getenv("AGOL_USERNAME")
AGOL_PASSWORD: os.getenv("AGOL_PASSWORD")
LAYER_ID_AGOL_METEOROLOGIA: os.getenv("LAYER_ID_AGOL_METEOROLOGIA")
URL_GIS_ENTERPRISE_GEONITEROI: os.getenv("URL_GIS_ENTERPRISE_GEONITEROI")

URL_RISCO_FOGO_API: os.getenv("URL_RISCO_FOGO_API")
URL_METEOROLOGIA_API: os.getenv("URL_METEOROLOGIA_API")


def execute():
    # Chamar dados api_risco
    response_risco = requests.get(URL_RISCO_FOGO_API)
    var_dict_risco = response_risco.__dict__
    content_risco = var_dict_risco['_content']

    # Utiliza o Json para transformar em dataframe
    obj1 = json.loads(content_risco)
    dfe0 = pd.DataFrame(obj1)
    dfe1 = pd.concat([dfe0.drop(['fields'], axis=1),
                     dfe0['fields'].apply(pd.Series)], axis=1)

    dfe1 = dfe1[dfe1['data_final'].isnull()]

    # Chamar dados api_meteorologica
    response_tempo = requests.get(URL_METEOROLOGIA_API)
    var_dict_tempo = response_tempo.__dict__
    content_tempo = var_dict_tempo['_content']

    # Divide a string content_tempo em colunas
    values = content_tempo.decode().split(';')  # decode de bytes para string

    # Ignora os 3 primieros e o último valor
    values = values[3:-1]

    # Cria o DataFrame com os valores restantes
    dft = pd.DataFrame([values], columns=['temperatura',
                       'umidade', 'vento', 'velocidade do vento', 'data'])

    dfe1.reset_index(drop=True, inplace=True)
    dft.reset_index(drop=True, inplace=True)

    # Adiciona os campos 'grav' e 'data_inicial' e seus valores
    dft['grav'] = dfe1['grav']
    dft['data_inicial'] = dfe1['data_inicial']

    # Converte 'data' e 'data_inicial' em objetos datetime
    dft['data'] = pd.to_datetime(dft['data'])
    dft['data_inicial'] = pd.to_datetime(dft['data_inicial'])

    # Adiciona 3 h em 'data' e 'data_inicial'
    dft['data'] = dft['data'] + timedelta(hours=3)
    dft['data_inicial'] = dft['data_inicial'] + timedelta(hours=3)

    # Converte as colunas de 'data' e 'data_inicial' em millisegundos, timestamp
    dft['data'] = dft['data'].apply(lambda x: int(x.timestamp() * 1000))
    dft['data_inicial'] = dft['data_inicial'].apply(
        lambda x: int(x.timestamp() * 1000))

    # Cria um geojson com os dados do dataframe dft
    geojson_features = []

    for index, row in dft.iterrows():
        feature = {
            "attributes": {
                "temperatura": row['temperatura'],
                "umidade": row['umidade'],
                "vento": row['vento'],
                "velocidade": row['velocidade do vento'],
                "risco": row['grav'],
                "inicial_risco": row['data_inicial'],
                "data": row['data']
            }
        }

        # Adicionar a feature à lista
        geojson_features.append(feature)

    # Função para atualizar os valores da camada existente
    url = URL_GIS_ENTERPRISE_GEONITEROI
    usr = AGOL_USERNAME
    pwd = AGOL_PASSWORD
    gis = GIS(url, usr, pwd, verify_cert=False)
    layer_id = LAYER_ID_AGOL_METEOROLOGIA

    # Localize a Feature Layer onde os dados estão armazenados (insira o seu Item ID)
    item = gis.content.get(layer_id)

    # Utiliza a primeira camada
    layer = item.layers[0]

    features = geojson_features

    for feature in features:
        # Constrói a expressão SQL para encontrar a feature correspondente
        expressao_sql = "objectid = 1"  # Atualiza apenas a linha com objectid 1

        # Cria um dicionário com os campos da Feature Layer e os valores correspondentes do GeoJSON
        valores_para_atualizar = {
            "temperatura": feature['attributes']['temperatura'],
            "umidade": feature['attributes']['umidade'],
            "vento": feature['attributes']['vento'],
            "velocidade": feature['attributes']['velocidade'],
            "risco": feature['attributes']['risco'],
            "inicial_risco": feature['attributes']['inicial_risco'],
            "data": feature['attributes']['data']
        }

        # Cria a lista de expressões para atualizar todos os campos de uma vez
        calc_expressions = [{"field": field_name, "value": value}
                            for field_name, value in valores_para_atualizar.items()]

        # Realiza a atualização de todos os campos
        layer.calculate(
            where=expressao_sql,
            calc_expression=calc_expressions
        )


def task(scheduler):
    scheduler.enter(3600, 1, task, (scheduler,))
    print('Atualizado', datetime.now())
    execute()


my_scheduler = sched.scheduler(time.time, time.sleep)
my_scheduler.enter(3600, 1, task, (my_scheduler,))
my_scheduler.run()

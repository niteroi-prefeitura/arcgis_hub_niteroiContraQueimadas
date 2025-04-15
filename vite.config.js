// vite.config.js
import { defineConfig } from 'vite'
export default defineConfig ({
  root: "src/app_estagio_incendios",
  base: "/defesa-civil-estagio/",
  build: {
    outDir: '../../dist',
    emptyOutDir: true,
  },
})
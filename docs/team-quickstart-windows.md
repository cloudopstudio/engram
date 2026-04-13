# Engram Cloud — Guia de Inicio Rapido (Windows)

> Tu equipo ya tiene engram configurado. Esta guia te ayuda a actualizar a la version cloud con autenticacion Azure.

## Paso 1: Descargar la nueva version

1. Descarga `engram_1.10.14-pg_windows_amd64.zip` desde:
   https://github.com/White-Lion-Technology/engram/releases/tag/v1.10.14-pg

2. Descomprime el archivo

3. Reemplaza tu `engram.exe` actual con el nuevo:
   ```powershell
   # Encuentra donde esta tu engram actual
   where engram

   # Copia el nuevo encima (ajusta la ruta segun tu caso)
   Copy-Item engram.exe -Destination (where engram) -Force
   ```

4. Verifica:
   ```powershell
   engram version
   # Deberia mostrar: engram 1.10.14-pg
   ```

## Paso 2: Configurar la conexion a Azure

Ejecuta estos comandos en PowerShell (reemplaza `<TU-EMAIL>` con tu correo corporativo):

```powershell
engram config set database-url "postgres://<TU-EMAIL>@whitelion-context-psqlserver-prod.postgres.database.azure.com:5432/engram?sslmode=require"
engram config set auth-method entra
engram config set tenant-id "<TENANT-ID>"
engram config set client-id "<CLIENT-ID>"
```

> Tu lider te proporcionara los valores de TENANT-ID y CLIENT-ID.

## Paso 3: Instalar el plugin de autenticacion en OpenCode

1. Abre el archivo de configuracion de OpenCode:
   ```powershell
   notepad $env:USERPROFILE\.config\opencode\opencode.json
   ```

2. Busca la linea `"plugin"` y agrega el plugin de Azure:
   ```json
   "plugin": [
     "opencode-gemini-auth@latest",
     "opencode-anthropic-login-via-cli",
     "file://C:/ruta/al/repo/engram/plugins/opencode-azure-entra-auth"
   ],
   ```

   > Pregunta a tu lider la ruta exacta del plugin, o si esta publicado como paquete npm.

3. En la seccion `"mcp"`, verifica que engram tenga esta configuracion:
   ```json
   "engram": {
     "command": ["engram", "mcp"],
     "enabled": true,
     "type": "local"
   }
   ```

4. Guarda y cierra el archivo.

## Paso 4: Primera autenticacion

1. Abre OpenCode
2. Se abrira tu navegador automaticamente con la pagina de login de Microsoft
3. Inicia sesion con tu correo corporativo (@whiteliontechnology.com)
4. Si te pide permisos, acepta
5. Veras un mensaje: **"Authenticated! You can close this tab."**
6. Vuelve a OpenCode — engram estara en verde

> **Solo necesitas hacer esto UNA VEZ.** El token se renueva automaticamente por ~90 dias.

## Verificar que funciona

En OpenCode, prueba escribir:
```
Busca en mi memoria algo sobre arquitectura
```

Si el agente usa `mem_search` y devuelve resultados, esta funcionando!

## Problemas comunes

| Problema | Solucion |
|----------|----------|
| "engram" no es un comando reconocido | Verifica que engram.exe esta en tu PATH |
| El browser no se abre | Ejecuta `engram login` manualmente en PowerShell |
| engram aparece en rojo en OpenCode | Cierra y vuelve a abrir OpenCode |
| "no cached Azure token" | Ejecuta `engram login` en PowerShell |
| "connection refused" | Verifica con tu lider que tu IP esta en la lista de Azure |

## Necesitas ayuda?

Contacta a tu lider de arquitectura.

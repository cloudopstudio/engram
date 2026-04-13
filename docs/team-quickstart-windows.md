# Engram Cloud — Guia de Inicio Rapido (Windows)

> Tu equipo ya tiene engram configurado. Esta guia te ayuda a conectarte a la base de datos compartida con autenticacion Azure.

## Paso 1: Descargar la nueva version

1. Descarga el archivo ZIP de la ultima version desde:
   https://github.com/Gentleman-Programming/engram/releases/latest

   Busca `engram_windows_amd64.zip` (o `arm64` si tienes un procesador ARM).

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
   ```

## Paso 2: Configurar la conexion a Azure

Ejecuta estos comandos en PowerShell. Tu lider te proporcionara los valores de `TENANT-ID`, `CLIENT-ID`, y la URL de la base de datos:

```powershell
engram config set database-url "postgres://<TU-EMAIL>@<SERVIDOR>.postgres.database.azure.com:5432/engram?sslmode=require"
engram config set tenant-id "<TENANT-ID>"
engram config set client-id "<CLIENT-ID>"
```

Reemplaza:
- `<TU-EMAIL>` — tu correo corporativo (ej. `juan@empresa.com`)
- `<SERVIDOR>` — el nombre del servidor PostgreSQL que te de tu lider
- `<TENANT-ID>` y `<CLIENT-ID>` — los UUIDs que te de tu lider

## Paso 3: Autenticarte

```powershell
engram login
```

Se abrira tu navegador automaticamente con la pagina de login de Microsoft. Inicia sesion con tu correo corporativo.

> **Si el navegador no se abre:** Aparecera una URL y un codigo corto en la terminal. Copia la URL, pégala en cualquier navegador, e ingresa el codigo.

> **Solo necesitas hacer esto UNA VEZ.** El token se renueva automaticamente por ~90 dias.

> **Problema con URLs largas en Windows:** Si la URL se corta en la terminal, usa **Windows Terminal** o **PowerShell 7** (pwsh). Tambien puedes copiar la parte de la URL que empieza con `https://login.microsoftonline.com/...` y pegarla manualmente en el navegador.

## Paso 4: Configurar OpenCode

```powershell
engram setup opencode
```

Este comando configura OpenCode automaticamente para usar engram. Reinicia OpenCode despues de ejecutarlo.

Si prefieres configurar manualmente, abre el archivo de configuracion de OpenCode:

```powershell
notepad $env:APPDATA\opencode\opencode.json
```

Y verifica que la seccion `"mcp"` tenga esta entrada:

```json
"engram": {
  "command": ["engram", "mcp"],
  "enabled": true,
  "type": "local"
}
```

> **Nota:** Si usas OpenCode como aplicacion de escritorio (no en terminal), el comando `engram login` debe ejecutarse desde PowerShell, no desde dentro de OpenCode.

## Paso 5: Verificar que funciona

Reinicia OpenCode y luego escribe:

```
Busca en mi memoria algo sobre arquitectura
```

Si el agente usa `mem_search` y devuelve resultados, esta funcionando!

## Uso diario

Una vez configurado, no necesitas hacer nada especial:

- Abre OpenCode normalmente
- El agente usa engram automaticamente
- Tus memorias se guardan en la base de datos compartida del equipo
- Despues de ~90 dias, ejecuta `engram login` de nuevo cuando se te pida

## Memorias personales vs del equipo

Cuando el agente guarda una memoria, puede ser:

- **`scope: project`** — visible para todo el equipo (valor por defecto)
- **`scope: personal`** — solo visible para ti (protegido a nivel de base de datos)

Si quieres compartir una memoria personal con el equipo:

```
mem_promote(id: <ID de la memoria>)
```

> Esto es **irreversible** — una vez compartida, no puede volver a ser privada.

## Problemas comunes

| Problema | Solucion |
|----------|----------|
| `engram` no es un comando reconocido | Verifica que `engram.exe` esta en una carpeta en tu PATH (ej. `%USERPROFILE%\go\bin`) |
| El browser no se abre | Ejecuta `engram login` en PowerShell; si sigue sin abrirse, usa el codigo Device Code que aparece |
| URL cortada en la terminal | Usa Windows Terminal o PowerShell 7 (pwsh) |
| `unknown key: tenant-id` | Actualiza a la ultima version de engram |
| `AADSTS900144: scope missing` | Verifica que `tenant-id` y `client-id` esten configurados correctamente |
| engram aparece en rojo en OpenCode | Cierra y vuelve a abrir OpenCode |
| "no cached Azure token" | Ejecuta `engram login` en PowerShell |
| "connection refused" | Verifica con tu lider que tu IP esta en la lista de Azure |

## Necesitas ayuda?

Contacta a tu lider de arquitectura o consulta la guia completa: [engram-cloud-setup.md](engram-cloud-setup.md)

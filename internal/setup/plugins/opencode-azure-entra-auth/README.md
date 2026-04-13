# opencode-azure-entra-auth

Azure Entra ID authentication plugin for OpenCode. Contains two independent plugins:

- **Server plugin** (`index.ts`) — Background token refresh on OpenCode startup via `loader()` hook
- **TUI plugin** (`tui.tsx`) — Interactive `/engram-login` slash command with device code flow

## Architecture

```
opencode.json → loads index.ts (server plugin)
  └── On startup: check token-cache.json → refresh if expired

tui.json → loads tui.tsx (TUI plugin)
  └── /engram-login: collect credentials → spawn `engram login` → device code UX
```

Both plugins coexist independently. The server plugin uses `@azure/identity` SDK for silent token refresh. The TUI plugin delegates to the `engram login` Go subprocess for interactive device code flow.

## How it works

### Server plugin (background token refresh)

```
OpenCode starts → plugin runs →
  ├── Check ~/.engram/token-cache.json
  │   ├── Valid token → done
  │   └── Expired → @azure/identity refreshes silently
  └── engram reads token-cache.json → connects to PostgreSQL
```

### TUI plugin (`/engram-login` slash command)

```
User types /engram-login →
  ├── Read ~/.engram/config.json
  │   ├── Multiple profiles? → DialogSelect to pick one
  │   ├── Missing tenant-id? → DialogPrompt (UUID validated)
  │   ├── Missing client-id? → DialogPrompt (UUID validated)
  │   └── Save new credentials via `engram config set`
  ├── Check token-cache.json
  │   └── Valid token? → DialogConfirm "Re-authenticate anyway?"
  ├── Spawn `engram login [--profile <name>]`
  │   ├── Parse stderr for device code URL + user code
  │   ├── Auto-open browser to microsoft.com/devicelogin
  │   └── Show busy dialog with device code
  └── On exit:
      ├── code 0 → success toast
      └── code ≠ 0 → error dialog with actionable message
```

## Prerequisites

1. **Azure App Registration** with:
   - Redirect URI: `http://localhost` (type: Mobile & Desktop)
   - API permissions: `https://ossrdbms-aad.database.windows.net/user_impersonation`
   - Under **Authentication** > **Advanced settings** > **Allow public client flows** = Yes

2. **engram binary** built with PostgreSQL support:
   ```bash
   go build -tags pgstore ./cmd/engram/
   ```

3. **engram configured** with tenant-id and client-id:
   ```bash
   engram config set tenant-id "your-azure-tenant-id"
   engram config set client-id "your-app-registration-client-id"
   ```
   Or configure via the `/engram-login` command — it will prompt for missing values.

## Installation

### Server plugin (opencode.json)

In `~/.config/opencode/opencode.json`:
```json
{
  "plugin": [
    "file:///path/to/engram/plugins/opencode-azure-entra-auth"
  ]
}
```

### TUI plugin (tui.json)

In `~/.config/opencode/tui.json`:
```json
{
  "$schema": "https://opencode.ai/tui.json",
  "plugin": [
    "file:///path/to/engram/plugins/opencode-azure-entra-auth"
  ]
}
```

**Note**: The TUI plugin uses `"exports": { "./tui": "./tui.tsx" }` in `package.json`. OpenCode resolves the TUI entry automatically from the exports field.

### Both plugins together

You can load both plugins simultaneously — they are independent:
- `opencode.json` → loads `./server` (index.ts) for background token refresh
- `tui.json` → loads `./tui` (tui.tsx) for the `/engram-login` slash command

## Configuration

The plugin reads configuration from engram's config file (`~/.engram/config.json`) using the same resolution chain as the Go CLI:

1. **Environment variables**: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`
2. **Profile config**: `~/.engram/config.json` > `profiles.{profile}.tenant-id`
3. **Root config**: `~/.engram/config.json` > `tenant-id`

Profile selection: `ENGRAM_PROFILE` env var or `default-profile` in config.json.

## Token cache format

The plugin writes to `~/.engram/token-cache.json`:

```json
{
  "access_token": "eyJ...",
  "expires_on": "2026-03-28T19:00:00.000Z"
}
```

This is read by engram's Go code to connect to Azure Database for PostgreSQL.

## Troubleshooting

### "engram not found"
The `engram` binary is not installed or not in your PATH. Build it:
```bash
go build -tags pgstore -o engram ./cmd/engram/
```

### "Build tag missing" / "pgstore build tag"
The engram binary was built without PostgreSQL support. Rebuild with:
```bash
go build -tags pgstore ./cmd/engram/
```

### "tenant-id and/or client-id not configured"
Run `/engram-login` — it will prompt for both values. Or configure manually:
```bash
engram config set tenant-id <id>
engram config set client-id <id>
```

### Browser doesn't open
Copy the URL from the dialog and open it manually. The device code is displayed in the dialog for manual entry.

### "Authentication timed out"
The Azure device code flow has a limited window. Run `/engram-login` again.

### Token refresh keeps failing (server plugin)
Delete the cache and re-authenticate via the TUI plugin:
```bash
rm ~/.engram/token-cache.json
```
Then run `/engram-login` in OpenCode.

### engram still can't connect after authentication
Make sure your Azure Database for PostgreSQL is configured to accept Azure AD authentication and your user has the right permissions.

## License

Apache-2.0

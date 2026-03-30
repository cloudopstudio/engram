[← Back to README](../README.md)

# Engram Cloud Setup Guide

PostgreSQL-backed engram for team collaboration with shared persistent memory.

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Azure PostgreSQL Setup](#azure-postgresql-setup)
- [Configuration Profiles](#configuration-profiles)
- [Authentication Methods](#authentication-methods)
- [MCP Client Configuration](#mcp-client-configuration)
- [CLI Reference](#cli-reference)
- [Migration from SQLite](#migration-from-sqlite)
- [Entra ID Token Lifecycle](#entra-id-token-lifecycle)
- [Troubleshooting](#troubleshooting)
- [Rollback to SQLite](#rollback-to-sqlite)

---

## Overview

**engram** ships with built-in PostgreSQL support. When configured with a `database-url`, it connects to PostgreSQL instead of using a local SQLite file. The entire team shares a single Azure Database for PostgreSQL — every `mem_save`, `mem_search`, and `mem_session_summary` reads and writes to the same database.

### Why Use It

| Feature | engram (SQLite) | engram (PostgreSQL) |
|---------|----------------|----------------------|
| Storage | Local `~/.engram/engram.db` per machine | Shared Azure PG instance |
| Collaboration | Git Sync (async, chunk-based) | Real-time shared memory |
| Search | FTS5 (no stemming) | pg_tsvector (stemming, phrases, exclusion) |
| Auth | None (local file) | Azure Entra ID (passwordless) or password |
| Audit trail | None | `created_by` on every record |
| Future | — | pgvector embeddings ready |

### Architecture

```
 ┌─────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────────┐
 │  Agent Arq              │  │  Agent DevOps            │  │  Agent Front             │
 │  (--profile arq)        │  │  (--profile devops)      │  │  (--profile front)       │
 │  engram                 │  │  engram                  │  │  engram                  │
 └───────────┬─────────────┘  └───────────┬──────────────┘  └───────────┬──────────────┘
             │                            │                             │
             │  Entra ID / Password       │  Entra ID / Password       │  Entra ID / Password
             │                            │                            │
             └────────────────────────────┼────────────────────────────┘
                                          │
                                   ┌──────▼──────┐
                                   │  Azure DB   │
                                   │  for PG     │
                                   │  (Flex)     │
                                   │             │
                                   │  TLS + AAD  │
                                   └─────────────┘
```

Each developer or agent runs `engram` locally with a **profile** that points to the right database. Profiles let you manage multiple databases (per-team, per-project, per-environment) from a single binary and config file.

---

## Prerequisites

| Requirement | Needed For | Install |
|-------------|-----------|---------|
| **Go 1.25+** | Building from source only | [go.dev/dl](https://go.dev/dl/) |
| **Azure CLI** (`az`) | Entra ID authentication (developers) | See [Auth Methods](#authentication-methods) |
| **Azure PG Flexible Server** | Database hosting | See [Azure PG Setup](#azure-postgresql-setup) |
| **Entra ID auth enabled** | Passwordless login | See [Azure PG Setup](#azure-postgresql-setup) |
| **Team members added as PG users** | Per-dev access | See [Azure PG Setup](#azure-postgresql-setup) |

> **Note:** If you're just evaluating locally with `docker run postgres`, you only need Docker and a connection string. Skip the Azure sections entirely.

---

## Installation

### From GitHub Release (recommended)

Download the prebuilt `engram` binary for your platform from [GitHub Releases](https://github.com/White-Lion-Technology/engram/releases). PostgreSQL support is built-in — no build tags needed.

#### macOS (Apple Silicon)

```bash
# Download the latest release
curl -L https://github.com/White-Lion-Technology/engram/releases/latest/download/engram_darwin_arm64.tar.gz -o engram.tar.gz
tar xzf engram.tar.gz
chmod +x engram
mv engram ~/.local/bin/

# Verify
engram version
```

#### macOS (Intel)

```bash
curl -L https://github.com/White-Lion-Technology/engram/releases/latest/download/engram_darwin_amd64.tar.gz -o engram.tar.gz
tar xzf engram.tar.gz
chmod +x engram
mv engram ~/.local/bin/

engram version
```

> **Tip:** Ensure `~/.local/bin` is in your `PATH`. Add `export PATH="$HOME/.local/bin:$PATH"` to your `~/.zshrc` or `~/.bashrc` if needed.

#### Linux (amd64)

```bash
curl -L https://github.com/White-Lion-Technology/engram/releases/latest/download/engram_linux_amd64.tar.gz -o engram.tar.gz
tar xzf engram.tar.gz
chmod +x engram
sudo mv engram /usr/local/bin/

engram version
```

#### Linux (arm64)

```bash
curl -L https://github.com/White-Lion-Technology/engram/releases/latest/download/engram_linux_arm64.tar.gz -o engram.tar.gz
tar xzf engram.tar.gz
chmod +x engram
sudo mv engram /usr/local/bin/

engram version
```

#### Windows (amd64)

```powershell
# Download the latest release
Invoke-WebRequest -Uri "https://github.com/White-Lion-Technology/engram/releases/latest/download/engram_windows_amd64.zip" -OutFile engram.zip
Expand-Archive engram.zip -DestinationPath .
Move-Item engram.exe "$env:USERPROFILE\go\bin\"

# Verify
engram.exe version
```

#### Windows (arm64)

```powershell
Invoke-WebRequest -Uri "https://github.com/White-Lion-Technology/engram/releases/latest/download/engram_windows_arm64.zip" -OutFile engram.zip
Expand-Archive engram.zip -DestinationPath .
Move-Item engram.exe "$env:USERPROFILE\go\bin\"

engram.exe version
```

> **Tip:** Ensure `%USERPROFILE%\go\bin` is in your `PATH`. Add it via **System Properties → Environment Variables** if needed.

### From Homebrew

```bash
brew install Gentleman-Programming/tap/engram
```

### Build from Source

Requires **Go 1.25+**.

```bash
git clone https://github.com/White-Lion-Technology/engram.git
cd engram
go build -tags pgstore -o engram ./cmd/engram/
```

The `-tags pgstore` flag activates the PostgreSQL store backend in addition to SQLite. The resulting `engram` binary auto-selects the backend based on configuration.

> **Version stamping** (optional):
> ```bash
> go build -tags pgstore -ldflags="-X main.version=local-$(git describe --tags --always)" -o engram ./cmd/engram/
> ```

---

## Azure PostgreSQL Setup

### Create an Azure Database for PostgreSQL Flexible Server

1. Go to the [Azure Portal](https://portal.azure.com) → **Create a resource** → **Azure Database for PostgreSQL Flexible Server**
2. Choose your subscription and resource group
3. Pick a server name (e.g., `engram-db`)
4. Select **PostgreSQL 16** (recommended — supports HNSW indexes for future pgvector)
5. Choose a compute tier appropriate for your team size (Burstable B1ms is fine for <10 devs)
6. Deploy

> **Full Azure docs:** [Create an Azure Database for PostgreSQL Flexible Server](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/quickstart-create-server-portal)

### Enable Entra ID Authentication

1. In your PG Flexible Server → **Authentication** blade
2. Set **Authentication type** to **Microsoft Entra authentication only** (or **PostgreSQL and Microsoft Entra authentication** if you also need password access)
3. Click **Add Microsoft Entra Admin** and add yourself as the initial admin
4. Save

> **Full Azure docs:** [Configure Microsoft Entra authentication](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/how-to-configure-sign-in-azure-ad-authentication)

### Create an Azure App Registration (for Device Code Flow)

If your team includes non-developer users (PMs, designers, etc.) who can't use `az login`, create an App Registration to enable Device Code Flow:

1. In [Azure Portal](https://portal.azure.com) → **Microsoft Entra ID** → **App registrations** → **New registration**
2. Name: `engram-device-code` (or any name)
3. Supported account types: **Accounts in this organizational directory only**
4. Redirect URI: leave blank (device code flow doesn't need one)
5. Click **Register**
6. Note the **Application (client) ID** — this is your `client-id`
7. Note the **Directory (tenant) ID** — this is your `tenant-id`
8. Go to **Authentication** → enable **Allow public client flows** → Save
9. Go to **API permissions** → **Add a permission** → **Azure OSSRDBMS Database** → **user_impersonation** → Grant admin consent

Share the `tenant-id` and `client-id` values with your team — they'll set them in their profiles.

### Add Team Members as PG Users

Each team member needs to be added as an Entra ID user on the PG server:

1. Connect to the PG server as the Entra admin (e.g., via `psql` or Azure Data Studio)
2. For each team member, run:

```sql
-- Replace with the team member's Entra ID email
SELECT * FROM pgaadauth_create_principal('dev-a@company.com', false, false);
GRANT ALL ON DATABASE engram TO "dev-a@company.com";
```

3. Create the `engram` database if it doesn't exist:

```sql
CREATE DATABASE engram;
```

> **Alternative:** Add team members as Entra ID admins in the Azure Portal (Authentication blade) — admins don't need the `pgaadauth_create_principal` step.

### Firewall Rules

Configure network access so team members can connect:

1. In your PG Flexible Server → **Networking** blade
2. **Option A: Public access** — add each developer's IP address (or office IP range) to the firewall rules
3. **Option B: Private access** — use Azure VNet integration or Private Endpoint if your team is on a corporate VPN
4. Enable **Allow public access from Azure services** if running engram from Azure VMs or managed identities

> **Warning:** Do NOT enable "Allow public access from any IP" (`0.0.0.0/0`) in production. Restrict to known IPs.

---

## Configuration Profiles

Profiles let you manage multiple PostgreSQL databases from a single `engram` installation. Each profile stores its own `database-url`, `auth-method`, `tenant-id`, and `client-id`.

Configuration is stored in `~/.engram/config.json` with the priority:

```
environment variable > profile value > root config value > default
```

### Create Profiles

```bash
# Create a profile for the architecture team
engram config set --profile arquitectura database-url "postgres://<your-email>@<your-server>.postgres.database.azure.com:5432/engram_arq?sslmode=require"
engram config set --profile arquitectura auth-method entra
engram config set --profile arquitectura tenant-id "<your-tenant-id>"
engram config set --profile arquitectura client-id "<your-client-id>"

# Create a profile for the devops team
engram config set --profile devops database-url "postgres://<your-email>@<your-server>.postgres.database.azure.com:5432/engram_devops?sslmode=require"
engram config set --profile devops auth-method entra
engram config set --profile devops tenant-id "<your-tenant-id>"
engram config set --profile devops client-id "<your-client-id>"

# Create a profile for local development (no Entra ID needed)
engram config set --profile local database-url "postgres://engram:password@localhost:5432/engram?sslmode=disable"
engram config set --profile local auth-method password
```

Both flag syntaxes work:

```bash
engram config set --profile=devops key value
engram config set --profile devops key value
```

### Set a Default Profile

```bash
engram config set default-profile arquitectura
```

When `default-profile` is set, all commands use it automatically — no need for `--profile` on every call.

### List Profiles

```bash
engram config profiles
```

Output:

```
Configured profiles:
  arquitectura (default)
  devops
  local
```

### View Profile Config

```bash
engram config list --profile arquitectura
```

### Delete a Profile

```bash
engram config delete-profile devops
```

If the deleted profile was the default, `default-profile` is cleared automatically.

### Config File Location

```bash
engram config path
# → /Users/<you>/.engram/config.json
```

### All Config Keys

| Key | Env Var | Default | Description |
|-----|---------|---------|-------------|
| `database-url` | `ENGRAM_DATABASE_URL` | — | PostgreSQL connection string |
| `auth-method` | `ENGRAM_AUTH_METHOD` | Auto-detected | `entra` or `password` |
| `server-port` | `ENGRAM_PORT` | `7437` | HTTP server port |
| `default-project` | — | — | Default project name for commands |
| `default-profile` | — | — | Default profile (used when `--profile` is omitted) |
| `tenant-id` | `AZURE_TENANT_ID` | — | Azure Entra ID tenant ID (for device code flow) |
| `client-id` | `AZURE_CLIENT_ID` | — | Azure app registration client ID (for device code flow) |

Keys that can be set inside profiles: `database-url`, `auth-method`, `server-port`, `tenant-id`, `client-id`.

Keys that are global-only (cannot be set inside profiles): `default-project`, `default-profile`.

---

## Authentication Methods

Engram supports three authentication methods for PostgreSQL:

| Method | Config | For Whom | Requirements |
|--------|--------|----------|-------------|
| **Password** | `auth-method: password` | Local dev/testing | Just `database-url` with credentials |
| **Azure CLI** | `auth-method: entra` | Developers (have `az login`) | Azure CLI installed + `az login` |
| **Device Code** | `auth-method: entra` + `--auth-interactive` | Non-devs (PMs, designers, agents) | `tenant-id` + `client-id` in config |

### Method 1: Password (local dev/testing)

For local PostgreSQL instances (Docker, Homebrew postgres, etc.) where Entra ID is not needed.

```bash
# Start a local PG (if you don't have one):
docker run -d --name engram-db -e POSTGRES_DB=engram -e POSTGRES_USER=engram -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:16-alpine

# Configure
engram config set --profile local database-url "postgres://engram:password@localhost:5432/engram?sslmode=disable"
engram config set --profile local auth-method password
engram config set default-profile local

# Test
engram stats
```

### Method 2: Azure CLI (developers)

Entra ID provides passwordless authentication. Each developer authenticates via `az login` — engram acquires a token automatically and uses it as the PG password.

#### macOS

```bash
brew install azure-cli
az login
```

#### Linux

```bash
# Debian/Ubuntu
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# RHEL/Fedora
# sudo dnf install azure-cli

az login
```

#### Windows

```powershell
winget install Microsoft.AzureCLI
az login
```

#### Configure

```bash
engram config set --profile cloud database-url "postgres://<your-email>@<your-server>.postgres.database.azure.com:5432/engram?sslmode=require"
engram config set --profile cloud auth-method entra
engram config set default-profile cloud
```

That's it. Engram uses `DefaultAzureCredential` which picks up the `az login` session automatically.

### Method 3: Device Code Flow (non-developers)

For users who don't have (or can't have) the Azure CLI — product managers, designers, or AI agents running in restricted environments. Instead of `az login`, the user authenticates via a browser-based device code flow.

#### How It Works

1. Engram prints a URL and a one-time code to stderr
2. The user opens the URL in any browser and enters the code
3. Azure authenticates the user and returns a token
4. The token is cached at `~/.engram/token-cache.json` (0600 permissions)
5. Cached tokens persist for ~60-90 minutes; the `az login` refresh lasts ~90 days

#### Prerequisites

The profile must have `tenant-id` and `client-id` set (from the Azure App Registration):

```bash
engram config set --profile arquitectura tenant-id "<your-tenant-id>"
engram config set --profile arquitectura client-id "<your-client-id>"
```

#### Pre-Authenticate (Interactive)

```bash
engram login --profile arquitectura
```

This triggers the device code flow immediately, caches the token, and exits. Useful to authenticate before starting an MCP session.

#### Auto-Authenticate on MCP Start

```bash
engram --profile arquitectura --auth-interactive mcp
```

When `--auth-interactive` is set and the cached token is expired or missing, engram triggers the device code flow automatically when the MCP server starts. The device code prompt is printed to stderr (stdout is reserved for the MCP protocol).

#### Token Cache

- **Location:** `~/.engram/token-cache.json`
- **Permissions:** `0600` (owner-only read/write)
- **Token validity:** ~60-90 minutes (Azure AD token lifetime)
- **Cache behavior:** If a valid (non-expired) cached token exists, it's used directly — no device code prompt
- **Refresh:** When expired, a new device code flow is triggered automatically (if `--auth-interactive` is set)

### Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `ENGRAM_DATABASE_URL` | PostgreSQL connection string (enables PG mode) | — |
| `ENGRAM_AUTH_METHOD` | `entra` or `password` | Auto-detected: `entra` for `*.database.azure.com`, `password` otherwise |
| `ENGRAM_MIGRATE_SOURCE` | Source SQLite DB for migration | `~/.engram/engram.db` |
| `ENGRAM_DATA_DIR` | Data directory (config, token cache, SQLite DB) | `~/.engram` |
| `ENGRAM_PORT` | HTTP server port | `7437` |
| `ENGRAM_FTS_LANGUAGE` | PostgreSQL text search language config | `english` |
| `AZURE_TENANT_ID` | Azure Entra ID tenant (overrides `tenant-id` config) | — |
| `AZURE_CLIENT_ID` | Azure app registration client (overrides `client-id` config) | — |

> **Note:** Environment variables override config values. Config profiles override root config values.

---

## MCP Client Configuration

With profiles, you no longer need wrapper scripts. Use `engram --profile <name> mcp` directly in your agent's MCP config.

### OpenCode

Edit `~/.config/opencode/opencode.json` (Windows: `%APPDATA%\opencode\opencode.json`):

```json
{
  "mcp": {
    "engram": {
      "type": "local",
      "command": ["engram", "--profile", "arquitectura", "mcp"],
      "enabled": true
    }
  }
}
```

#### Multiple Agents with Different Profiles

```json
{
  "mcp": {
    "engram-arq": {
      "type": "local",
      "command": ["engram", "--profile", "arquitectura", "mcp"],
      "enabled": true
    },
    "engram-devops": {
      "type": "local",
      "command": ["engram", "--profile", "devops", "--auth-interactive", "mcp"],
      "enabled": true
    }
  }
}
```

### Claude Code

**Option A: Via `claude mcp add`:**

```bash
claude mcp add engram -- engram --profile arquitectura mcp
```

**Option B: Manual config** — add to `.claude/settings.json` (project) or `~/.claude/settings.json` (global):

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["--profile", "arquitectura", "mcp"]
    }
  }
}
```

#### With Device Code Auth (non-dev users)

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["--profile", "arquitectura", "--auth-interactive", "mcp"]
    }
  }
}
```

### Gemini CLI

Edit `~/.gemini/settings.json` (Windows: `%APPDATA%\gemini\settings.json`):

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["--profile", "arquitectura", "mcp"]
    }
  }
}
```

### VS Code (Copilot / Claude Code Extension)

Add to `.vscode/mcp.json` (workspace) or user-level `mcp.json`:

```json
{
  "servers": {
    "engram": {
      "command": "engram",
      "args": ["--profile", "arquitectura", "mcp"]
    }
  }
}
```

### Codex

Edit `~/.codex/config.toml` (Windows: `%APPDATA%\codex\config.toml`):

```toml
[mcp_servers.engram]
command = "engram"
args = ["--profile", "arquitectura", "mcp"]
```

### MCP Tool Profiles

The `mcp` command supports a `--tools` flag to limit which MCP tools are exposed:

```bash
engram mcp --tools=agent    # 11 core tools (recommended for agents)
engram mcp --tools=admin    # 3 admin tools only
engram mcp --tools=all      # All 14 tools (default)
```

Combine with profiles:

```bash
engram --profile arquitectura mcp --tools=agent
```

### Without Profiles (Legacy Wrapper Scripts)

If you prefer environment variables over profiles, you can still use wrapper scripts:

```bash
#!/bin/bash
export ENGRAM_DATABASE_URL="postgres://<your-email>@<your-server>.postgres.database.azure.com:5432/engram?sslmode=require"
export ENGRAM_AUTH_METHOD=entra
exec engram "$@"
```

Then use the wrapper in your MCP config. However, **profiles are the recommended approach** — they're simpler, portable, and don't leak credentials in shell scripts.

---

## CLI Reference

### Global Flags

```
engram [--profile NAME] [--auth-interactive] <command> [arguments]

Flags:
  --profile NAME        Use a specific config profile (overrides default-profile)
  --profile=NAME        Same, with equals syntax
  --auth-interactive    Enable Azure device code login (for non-dev users)
```

### All Commands

| Command | Description |
|---------|-------------|
| `serve [port]` | Start HTTP API server (default: 7437) |
| `mcp [--tools=PROFILE]` | Start MCP server (stdio transport, for any AI agent) |
| `tui` | Launch interactive terminal UI (Bubbletea-based) |
| `search <query>` | Search memories `[--type TYPE] [--project PROJECT] [--scope SCOPE] [--limit N]` |
| `save <title> <msg>` | Save a memory `[--type TYPE] [--project PROJECT] [--scope SCOPE] [--topic TOPIC_KEY]` |
| `timeline <obs_id>` | Show chronological context around an observation `[--before N] [--after N]` |
| `context [project]` | Show recent context from previous sessions `[--scope SCOPE]` |
| `stats` | Show memory system statistics |
| `export [file]` | Export all memories to JSON (default: `engram-export.json`) |
| `import <file>` | Import memories from a JSON export file |
| `migrate` | Migrate data from SQLite to PostgreSQL |
| `login` | Authenticate with Azure (device code flow), caches token |
| `config set [--profile NAME] <key> <value>` | Set a configuration value |
| `config get [--profile NAME] <key>` | Get a configuration value (shows source) |
| `config list [--profile NAME]` | List all configuration with sources |
| `config profiles` | List all configured profiles |
| `config delete-profile <name>` | Delete a profile from config |
| `config path` | Print config file path |
| `setup [agent]` | Install agent integration (`opencode`, `claude-code`, `gemini-cli`, `codex`) |
| `sync` | Export new memories as compressed chunk to `.engram/` |
| `sync --import` | Import new chunks from `.engram/` into local DB |
| `sync --status` | Show sync status (local vs remote chunks) |
| `version` | Print version |
| `help` | Show help |

---

## Migration from SQLite

Migrate your existing local engram memories to the shared PostgreSQL database. The migration is **idempotent** — safe to re-run.

### macOS / Linux

```bash
# Set source (your existing engram DB — defaults to ~/.engram/engram.db)
export ENGRAM_MIGRATE_SOURCE="$HOME/.engram/engram.db"

# Authenticate (pick one method)
az login                                          # Method 2: Azure CLI
engram login --profile cloud                      # Method 3: Device Code

# Run migration (uses database-url from your profile)
engram --profile cloud migrate

# Verify
engram --profile cloud search --query "test" --project <your-project>
```

### Windows

```powershell
# Set source
$env:ENGRAM_MIGRATE_SOURCE = "$env:USERPROFILE\.engram\engram.db"

# Authenticate
az login

# Run migration
engram.exe --profile cloud migrate

# Verify
engram.exe --profile cloud search --query "test" --project <your-project>
```

### What Gets Migrated

| Data | Migrated | Notes |
|------|----------|-------|
| Sessions | Yes | `ON CONFLICT(id) DO NOTHING` — safe to re-run |
| Observations (including soft-deleted) | Yes | `ON CONFLICT(sync_id) DO NOTHING` — idempotent |
| User prompts | Yes | `ON CONFLICT(sync_id) DO NOTHING` — idempotent |
| Sync tables (chunks, state, mutations) | No | PG sync tables start fresh |
| FTS indexes | N/A | PG tsvector triggers auto-populate on insert |

### Migration Details

- **Batch size:** 500 rows per batch (progress reported every 5,000 observations)
- **created_by:** All migrated records are stamped with your Entra ID email
- **Validation:** After migration, row counts are compared between source and target
- **Source is read-only:** Your SQLite database is never modified or deleted

> **Warning:** If you have 50,000+ observations, the migration may take a few minutes. The progress indicator updates every 5,000 rows.

---

## Entra ID Token Lifecycle

Understanding how token refresh works prevents authentication surprises.

### How It Works (Azure CLI — Method 2)

```
  az login (one-time)
       │
       ▼
  Azure CLI caches credentials locally
       │
       ▼
  engram starts ──► NewTokenProvider()
       │                 uses azidentity.DefaultAzureCredential
       │                 (checks: CLI cache → Managed Identity → Env Vars)
       │
       ▼
  MCP tool call (e.g., mem_save)
       │
       ▼
  pgx pool needs a connection
       │
       ▼
  BeforeConnect hook fires ──► TokenProvider.Token()
       │                         │
       │    ┌────────────────────┤
       │    │                    │
       │    ▼                    ▼
       │  Token valid?        Token expired or
       │  (>5 min to         within 5 min of expiry?
       │   expiry)              │
       │    │                   ▼
       │    │            Acquire new token
       │    │            from Azure AD
       │    │                   │
       │    ▼                   ▼
       │  Use cached         Use new token
       │  token              (cache updated)
       │    │                   │
       └────┴───────────────────┘
                    │
                    ▼
            Token used as PG password
                    │
                    ▼
            PG validates against Azure AD
                    │
                    ▼
            Connection established (TLS)
```

### How It Works (Device Code — Method 3)

```
  engram --auth-interactive mcp
       │
       ▼
  Check ~/.engram/token-cache.json
       │
       ├─── Valid token? ──► Use cached token (no prompt)
       │
       └─── Expired/missing?
              │
              ▼
        Print device code to stderr:
        "To sign in, visit https://microsoft.com/devicelogin
         and enter code XXXXXXXX"
              │
              ▼
        User completes browser flow
              │
              ▼
        Token acquired + cached to token-cache.json
              │
              ▼
        Normal BeforeConnect hook (same as above)
```

### Key Details

- **Token validity:** Azure AD tokens last ~60-90 minutes
- **Refresh threshold:** engram refreshes tokens **5 minutes before expiry** — no interruptions during normal use
- **Caching:** All connections in the pool share the same cached token — only one Azure AD request per refresh cycle
- **Thread safety:** Multiple concurrent MCP tool calls safely share the token (protected by sync.RWMutex with double-check locking)
- **Connection pool:** Max 5 connections, max lifetime 30 minutes (rotated before token expiry)
- **File cache (device code):** Token persisted at `~/.engram/token-cache.json` — survives process restarts
- **Credential chain (Azure CLI):** `DefaultAzureCredential` tries (in order): Azure CLI → Managed Identity → Environment Variables → Visual Studio Code

### When Does Authentication Expire?

| Method | Token Lifetime | Refresh |
|--------|---------------|---------|
| Azure CLI (`az login`) | Refresh token: ~90 days | Run `az login` again when expired |
| Device Code | Access token: ~60-90 min | Auto-prompted if `--auth-interactive` is set |
| Device Code (cached) | File cache: until token expires | New device code flow triggered |

- If you use `az login` daily, you'll almost never be prompted again
- CI/CD: Use managed identity or service principal instead of `az login`

---

## Troubleshooting

### Connection Refused

```
engram: connect to PG: dial tcp <ip>:5432: connect: connection refused
```

**Cause:** PostgreSQL server is not reachable.

**Fix:**
- **Azure PG:** Check firewall rules — your IP may not be allowed. Go to Azure Portal → your PG server → Networking → add your current IP.
- **Local PG:** Ensure PostgreSQL is running: `pg_isready` (macOS/Linux) or check Services (Windows).
- **Docker:** Ensure the container is running: `docker ps | grep postgres`

### Authentication Failed

```
engram: entra token for pg connection: no Azure credential available
```

**Cause:** `az login` session expired or was never initiated.

**Fix:**
```bash
# Method 2: Azure CLI
az login

# Method 3: Device Code
engram login --profile <your-profile>
```

If you're using password auth and see `password authentication failed`:
- Verify the password in your `database-url` is correct
- Check that the user exists on the PG server

### Device Code Auth Fails

```
engram: device code auth requires tenant-id and client-id.
```

**Cause:** Missing `tenant-id` or `client-id` in your profile config.

**Fix:**
```bash
engram config set --profile <name> tenant-id "<your-tenant-id>"
engram config set --profile <name> client-id "<your-client-id>"
```

Get these values from the Azure App Registration (see [Create an Azure App Registration](#create-an-azure-app-registration-for-device-code-flow)).

### Profile Not Found Warning

```
engram: warning: profile "xyz" not found in config, using root config
```

**Cause:** The profile name doesn't exist in `config.json`.

**Fix:**
```bash
# List existing profiles
engram config profiles

# Create the missing profile
engram config set --profile xyz database-url "postgres://..."
```

### Permission Denied

```
ERROR: permission denied for table observations
```

**Cause:** Your Entra ID user hasn't been granted access to the `engram` database.

**Fix:** Ask your PG admin to run:
```sql
SELECT * FROM pgaadauth_create_principal('your-email@company.com', false, false);
GRANT ALL ON DATABASE engram TO "your-email@company.com";
GRANT ALL ON ALL TABLES IN SCHEMA public TO "your-email@company.com";
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "your-email@company.com";
```

### Certificate / SSL Error

```
engram: connect to PG: tls: ... certificate
```

**Cause:** SSL/TLS configuration mismatch.

**Fix:**
- Azure PG **requires** TLS. Ensure `sslmode=require` is in your `database-url`:
  ```
  postgres://user@server.postgres.database.azure.com:5432/engram?sslmode=require
  ```
- For local dev with Docker, use `sslmode=disable`:
  ```
  postgres://engram:password@localhost:5432/engram?sslmode=disable
  ```

### Migrate Command Not Available

```
engram: 'migrate' command requires the pgstore build tag.
  Rebuild with: go build -tags pgstore ./cmd/engram/
```

**Cause:** You built engram from source without the `pgstore` build tag.

**Fix:** Rebuild with the pgstore tag:
```bash
go build -tags pgstore -o engram ./cmd/engram/
```

> **Note:** Pre-built release binaries always include PostgreSQL support. This error only occurs with custom source builds.

### Windows: PATH Issues

**Symptom:** MCP client can't find `engram`.

**Fix:**
- Ensure the directory containing `engram.exe` is in your system `PATH`
- In MCP configs, use the **full path** if PATH resolution fails:
  ```json
  {
    "command": "C:\\Users\\<user>\\go\\bin\\engram.exe",
    "args": ["--profile", "arquitectura", "mcp"]
  }
  ```

### Linux: Azure CLI Installation Variants

Different package managers install `az` to different paths. If `az login` gives "command not found":

| Distro | Install | Path |
|--------|---------|------|
| Debian/Ubuntu | `curl -sL https://aka.ms/InstallAzureCLIDeb \| sudo bash` | `/usr/bin/az` |
| RHEL/Fedora | `sudo dnf install azure-cli` | `/usr/bin/az` |
| Snap | `sudo snap install azure-cli --classic` | `/snap/bin/az` |
| pip | `pip install azure-cli` | `~/.local/bin/az` |

Ensure the installed path is in your `PATH`.

---

## Rollback to SQLite

If you need to switch back to the standard SQLite-based engram:

### 1. Remove Profile or Default

```bash
# Option A: Delete the cloud profile entirely
engram config delete-profile cloud

# Option B: Switch default profile to a local SQLite setup
engram config set default-profile ""
```

Or simply unset the environment variable if you were using one:

```bash
unset ENGRAM_DATABASE_URL
```

### 2. Your Local Data Is Untouched

The SQLite database at `~/.engram/engram.db` (Windows: `%USERPROFILE%\.engram\engram.db`) was **never modified** by the PG migration or by running engram in PG mode. It's still there with all your pre-migration data.

### 3. Export from PG (Optional)

If you accumulated new data in PostgreSQL that you want to keep locally:

```bash
# Export from PG to JSON (with profile that has database-url)
engram --profile cloud export engram-backup.json

# Import into local SQLite (without any PG profile active)
engram import engram-backup.json
```

### 4. Verify

```bash
engram search --query "test" --project <your-project>
engram stats
```

You should see your original local memories plus any imported from PG.

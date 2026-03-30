[← Back to README](../README.md)

# Engram Cloud Setup Guide

PostgreSQL-backed engram for team collaboration with shared persistent memory.

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Azure PostgreSQL Setup](#azure-postgresql-setup)
- [Authentication Configuration](#authentication-configuration)
- [MCP Client Configuration](#mcp-client-configuration)
- [Migration from SQLite](#migration-from-sqlite)
- [Entra ID Token Lifecycle](#entra-id-token-lifecycle)
- [Troubleshooting](#troubleshooting)
- [Rollback to SQLite](#rollback-to-sqlite)

---

## Overview

**engram** ships with built-in PostgreSQL support. When configured with `ENGRAM_DATABASE_URL`, it connects to PostgreSQL instead of using a local SQLite file. The entire team shares a single Azure Database for PostgreSQL — every `mem_save`, `mem_search`, and `mem_session_summary` reads and writes to the same database.

### Why Use It

| Feature | engram (SQLite) | engram (PostgreSQL) |
|---------|----------------|----------------------|
| Storage | Local `~/.engram/engram.db` per machine | Shared Azure PG instance |
| Collaboration | Git Sync (async, chunk-based) | Real-time shared memory |
| Search | FTS5 (no stemming) | pg_tsvector (stemming, phrases, exclusion) |
| Auth | None (local file) | Azure Entra ID (passwordless) |
| Audit trail | None | `created_by` on every record |
| Future | — | pgvector embeddings ready |

### Architecture

```
 ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
 │  Dev A       │   │  Dev B       │   │  Dev C       │
 │  (macOS)     │   │  (Linux)     │   │  (Windows)   │
 │  engram      │   │  engram      │   │  engram      │
 └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
        │                  │                   │
        │    Entra ID      │    Entra ID       │    Entra ID
        │    token auth    │    token auth     │    token auth
        │                  │                   │
        └──────────────────┼───────────────────┘
                           │
                    ┌──────▼──────┐
                    │  Azure DB   │
                    │  for PG     │
                    │  (Flex)     │
                    │             │
                    │  TLS + AAD  │
                    └─────────────┘
```

Each developer runs `engram` locally. When `ENGRAM_DATABASE_URL` is set, the binary connects to Azure Database for PostgreSQL using Entra ID tokens (acquired via `az login`). No passwords are stored — authentication is fully managed by Azure AD.

---

## Prerequisites

| Requirement | Needed For | Install |
|-------------|-----------|---------|
| **Go 1.25+** | Building from source only | [go.dev/dl](https://go.dev/dl/) |
| **Azure CLI** (`az`) | Entra ID authentication | See [Section 5a](#5a-with-entra-id-recommended-for-teams) |
| **Azure PG Flexible Server** | Database hosting | See [Section 4](#azure-postgresql-setup) |
| **Entra ID auth enabled** | Passwordless login | See [Section 4](#azure-postgresql-setup) |
| **Team members added as PG users** | Per-dev access | See [Section 4](#azure-postgresql-setup) |

> **Note:** If you're just evaluating locally with `docker run postgres`, you only need Docker and a connection string. Skip the Azure sections entirely.

---

## Installation

### From GitHub Release (recommended)

Download the prebuilt `engram` binary for your platform from [GitHub Releases](https://github.com/Gentleman-Programming/engram/releases). PostgreSQL support is built-in.

#### macOS (Apple Silicon)

```bash
# Download the latest release
curl -L https://github.com/Gentleman-Programming/engram/releases/latest/download/engram_darwin_arm64.tar.gz -o engram.tar.gz
tar xzf engram.tar.gz
chmod +x engram
mv engram ~/.local/bin/

# Verify
engram version
```

#### macOS (Intel)

```bash
curl -L https://github.com/Gentleman-Programming/engram/releases/latest/download/engram_darwin_amd64.tar.gz -o engram.tar.gz
tar xzf engram.tar.gz
chmod +x engram
mv engram ~/.local/bin/

engram version
```

> **Tip:** Ensure `~/.local/bin` is in your `PATH`. Add `export PATH="$HOME/.local/bin:$PATH"` to your `~/.zshrc` or `~/.bashrc` if needed.

#### Linux (amd64)

```bash
curl -L https://github.com/Gentleman-Programming/engram/releases/latest/download/engram_linux_amd64.tar.gz -o engram.tar.gz
tar xzf engram.tar.gz
chmod +x engram
sudo mv engram /usr/local/bin/

engram version
```

#### Linux (arm64)

```bash
curl -L https://github.com/Gentleman-Programming/engram/releases/latest/download/engram_linux_arm64.tar.gz -o engram.tar.gz
tar xzf engram.tar.gz
chmod +x engram
sudo mv engram /usr/local/bin/

engram version
```

#### Windows (amd64)

```powershell
# Download the latest release
Invoke-WebRequest -Uri "https://github.com/Gentleman-Programming/engram/releases/latest/download/engram_windows_amd64.zip" -OutFile engram.zip
Expand-Archive engram.zip -DestinationPath .
Move-Item engram.exe "$env:USERPROFILE\go\bin\"

# Verify
engram.exe version
```

#### Windows (arm64)

```powershell
Invoke-WebRequest -Uri "https://github.com/Gentleman-Programming/engram/releases/latest/download/engram_windows_arm64.zip" -OutFile engram.zip
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
git clone https://github.com/Gentleman-Programming/engram.git
cd engram
go build -tags pgstore -o engram ./cmd/engram/
```

The `-tags pgstore` flag activates the PostgreSQL store backend in addition to SQLite. The resulting `engram` binary auto-selects the backend based on whether `ENGRAM_DATABASE_URL` is set.

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

## Authentication Configuration

### 5a. With Entra ID (recommended for teams)

Entra ID provides passwordless authentication. Each developer authenticates via `az login` — engram acquires a token automatically and uses it as the PG password.

#### macOS

```bash
# Install Azure CLI
brew install azure-cli

# Login (one-time, or when session expires — tokens last ~1 hour)
az login

# Create a wrapper script for convenience
mkdir -p ~/.local/bin
cat > ~/.local/bin/engram-cloud << 'EOF'
#!/bin/bash
export ENGRAM_DATABASE_URL="postgres://<your-email>@<server>.postgres.database.azure.com:5432/engram?sslmode=require"
export ENGRAM_AUTH_METHOD=entra
exec engram "$@"
EOF
chmod +x ~/.local/bin/engram-cloud
```

Replace:
- `<your-email>` — your Entra ID email (e.g., `dev-a@company.com`)
- `<server>` — your Azure PG server name (e.g., `engram-db`)

#### Linux

```bash
# Install Azure CLI (Debian/Ubuntu)
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Or for RHEL/CentOS/Fedora:
# sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc
# sudo dnf install azure-cli

# Login
az login

# Create wrapper script
mkdir -p ~/.local/bin
cat > ~/.local/bin/engram-cloud << 'EOF'
#!/bin/bash
export ENGRAM_DATABASE_URL="postgres://<your-email>@<server>.postgres.database.azure.com:5432/engram?sslmode=require"
export ENGRAM_AUTH_METHOD=entra
exec engram "$@"
EOF
chmod +x ~/.local/bin/engram-cloud
```

> **Other Linux distros:** See [Install the Azure CLI on Linux](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-linux) for apt, yum, zypper, and manual install options.

#### Windows (PowerShell)

```powershell
# Install Azure CLI
winget install Microsoft.AzureCLI

# Login
az login

# Create wrapper script
# Save as: C:\Users\<user>\go\bin\engram-cloud.cmd
@echo off
set ENGRAM_DATABASE_URL=postgres://<your-email>@<server>.postgres.database.azure.com:5432/engram?sslmode=require
set ENGRAM_AUTH_METHOD=entra
engram.exe %*
```

Save the file above as `engram-cloud.cmd` in a directory that's in your `PATH` (e.g., `%USERPROFILE%\go\bin\`).

> **PowerShell alternative** — create `engram-cloud.ps1`:
> ```powershell
> $env:ENGRAM_DATABASE_URL = "postgres://<your-email>@<server>.postgres.database.azure.com:5432/engram?sslmode=require"
> $env:ENGRAM_AUTH_METHOD = "entra"
> & engram.exe @args
> ```
> Note: `.ps1` scripts require `pwsh -File engram-cloud.ps1` to invoke — `.cmd` is simpler for MCP config.

### 5b. With Password (for local dev/testing)

For local PostgreSQL instances (Docker, Homebrew postgres, etc.) where Entra ID is not needed.

#### macOS / Linux

```bash
# Start a local PG (if you don't have one):
# docker run -d --name engram-db -e POSTGRES_DB=engram -e POSTGRES_USER=engram -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:16-alpine

# Create wrapper script
mkdir -p ~/.local/bin
cat > ~/.local/bin/engram-local << 'EOF'
#!/bin/bash
export ENGRAM_DATABASE_URL="postgres://engram:password@localhost:5432/engram?sslmode=disable"
export ENGRAM_AUTH_METHOD=password
exec engram "$@"
EOF
chmod +x ~/.local/bin/engram-local
```

#### Windows

Save as `engram-local.cmd` in a directory in your `PATH`:

```cmd
@echo off
set ENGRAM_DATABASE_URL=postgres://engram:password@localhost:5432/engram?sslmode=disable
set ENGRAM_AUTH_METHOD=password
engram.exe %*
```

### Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `ENGRAM_DATABASE_URL` | PostgreSQL connection string (enables PG mode) | — |
| `ENGRAM_AUTH_METHOD` | `entra` or `password` | Auto-detected: `entra` for `*.database.azure.com`, `password` otherwise |
| `ENGRAM_MIGRATE_SOURCE` | Source SQLite DB for migration | `~/.engram/engram.db` |
| `ENGRAM_DATA_DIR` | Data directory (used for default migration source path) | `~/.engram` |
| `ENGRAM_PORT` | HTTP server port | `7437` |
| `ENGRAM_FTS_LANGUAGE` | PostgreSQL text search language config | `english` |

---

## MCP Client Configuration

Configure your AI agent to use `engram-cloud` (or `engram-local`) instead of plain `engram` to auto-set the database URL. Alternatively, set `ENGRAM_DATABASE_URL` in your environment and use `engram` directly.

### OpenCode

Edit `~/.config/opencode/opencode.json` (Windows: `%APPDATA%\opencode\opencode.json`):

```json
{
  "mcp": {
    "engram": {
      "type": "local",
      "command": ["engram-cloud", "mcp"],
      "enabled": true
    }
  }
}
```

### Claude Code

**Option A: Via `claude mcp add`:**

```bash
claude mcp add engram -- engram-cloud mcp
```

**Option B: Manual config** — add to `.claude/settings.json` (project) or `~/.claude/settings.json` (global):

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram-cloud",
      "args": ["mcp"]
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
      "command": "engram-cloud",
      "args": ["mcp"]
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
      "command": "engram-cloud",
      "args": ["mcp"]
    }
  }
}
```

### Codex

Edit `~/.codex/config.toml` (Windows: `%APPDATA%\codex\config.toml`):

```toml
[mcp_servers.engram]
command = "engram-cloud"
args = ["mcp"]
```

### Any Other MCP Agent

The pattern is always the same — use `engram-cloud` (or `engram-local`) in your agent's MCP config. These wrapper scripts just set `ENGRAM_DATABASE_URL` and call `engram`. The `mcp` subcommand starts the MCP server on stdio, identical to the SQLite version.

---

## Migration from SQLite

Migrate your existing local engram memories to the shared PostgreSQL database. The migration is **idempotent** — safe to re-run.

### macOS / Linux

```bash
# Set source (your existing engram DB — defaults to ~/.engram/engram.db)
export ENGRAM_MIGRATE_SOURCE="$HOME/.engram/engram.db"

# Set target
export ENGRAM_DATABASE_URL="postgres://<your-email>@<server>.postgres.database.azure.com:5432/engram?sslmode=require"
export ENGRAM_AUTH_METHOD=entra

# Authenticate with Azure (if not already logged in)
az login

# Run migration
engram migrate

# Verify
engram search --query "test" --project <your-project>
```

### Windows

```powershell
# Set source
$env:ENGRAM_MIGRATE_SOURCE = "$env:USERPROFILE\.engram\engram.db"

# Set target
$env:ENGRAM_DATABASE_URL = "postgres://<your-email>@<server>.postgres.database.azure.com:5432/engram?sslmode=require"
$env:ENGRAM_AUTH_METHOD = "entra"

# Authenticate
az login

# Run migration
engram.exe migrate

# Verify
engram.exe search --query "test" --project <your-project>
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

### How It Works

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

### Key Details

- **Token validity:** Azure AD tokens last ~60-90 minutes
- **Refresh threshold:** engram refreshes tokens **5 minutes before expiry** — no interruptions during normal use
- **Caching:** All connections in the pool share the same cached token — only one Azure AD request per refresh cycle
- **Thread safety:** Multiple concurrent MCP tool calls safely share the token (protected by sync.RWMutex with double-check locking)
- **Connection pool:** Max 5 connections, max lifetime 30 minutes (rotated before token expiry)
- **Credential chain:** `DefaultAzureCredential` tries (in order): Azure CLI → Managed Identity → Environment Variables → Visual Studio Code

### When Does `az login` Expire?

- Azure CLI refresh tokens last **90 days** (or until revoked)
- If you use `az login` daily, you'll almost never be prompted again
- If the refresh token expires, run `az login` again — one-time browser flow
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
az login
# Then retry your engram command
```

If you're using password auth and see `password authentication failed`:
- Verify the password in your `ENGRAM_DATABASE_URL` is correct
- Check that the user exists on the PG server

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
- Azure PG **requires** TLS. Ensure `sslmode=require` is in your `ENGRAM_DATABASE_URL`:
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

**Symptom:** MCP client can't find `engram-cloud`.

**Fix:**
- Ensure the directory containing `engram-cloud.cmd` is in your system `PATH`
- In MCP configs, use the **full path** if PATH resolution fails:
  ```json
  {
    "command": "C:\\Users\\<user>\\go\\bin\\engram-cloud.cmd",
    "args": ["mcp"]
  }
  ```

### Windows: .cmd vs .ps1 Wrapper

- **`.cmd`** — works everywhere (CMD, PowerShell, MCP clients). Recommended.
- **`.ps1`** — requires `pwsh -File wrapper.ps1` invocation. Some MCP clients don't support this.
- If using `.ps1`, configure MCP as:
  ```json
  {
    "command": "pwsh",
    "args": ["-File", "C:\\Users\\<user>\\go\\bin\\engram-cloud.ps1", "mcp"]
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

### ENGRAM_DATABASE_URL Must Be Set

```
engram: ENGRAM_DATABASE_URL must be set for PostgreSQL mode
```

**Cause:** engram requires a connection string for PostgreSQL mode.

**Fix:** Set the environment variable before running engram, or use a wrapper script (see [Section 5](#authentication-configuration)).

---

## Rollback to SQLite

If you need to switch back to the standard SQLite-based engram:

### 1. Change MCP Config

Remove the `ENGRAM_DATABASE_URL` environment variable — use `engram` directly in your agent's MCP config:

```json
{
  "command": "engram",
  "args": ["mcp"]
}
```

Or for OpenCode:

```json
{
  "mcp": {
    "engram": {
      "type": "local",
      "command": ["engram", "mcp"],
      "enabled": true
    }
  }
}
```

### 2. Your Local Data Is Untouched

The SQLite database at `~/.engram/engram.db` (Windows: `%USERPROFILE%\.engram\engram.db`) was **never modified** by the PG migration or by running engram in PG mode. It's still there with all your pre-migration data.

### 3. Export from PG (Optional)

If you accumulated new data in PostgreSQL that you want to keep locally:

```bash
# Export from PG to JSON (with ENGRAM_DATABASE_URL set)
engram export engram-backup.json

# Import into local SQLite (without ENGRAM_DATABASE_URL)
engram import engram-backup.json
```

### 4. Verify

```bash
engram search --query "test" --project <your-project>
engram stats
```

You should see your original local memories plus any imported from PG.

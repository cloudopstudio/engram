[← Back to README](../README.md)

# Engram Cloud Setup Guide

PostgreSQL-backed engram for team collaboration with shared persistent memory.

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Azure PostgreSQL Setup](#azure-postgresql-setup)
- [Authentication Configuration](#authentication-configuration)
- [Team Onboarding Quickstart](#team-onboarding-quickstart)
- [Personal vs Project Scope](#personal-vs-project-scope)
- [Team Collaboration Tools](#team-collaboration-tools)
- [MCP Client Configuration](#mcp-client-configuration)
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

Each developer runs `engram` locally. When a `database-url` is configured, the binary connects to Azure Database for PostgreSQL using Entra ID tokens. No passwords are stored — authentication is fully managed by Azure AD.

---

## Prerequisites

| Requirement | Needed For | Install |
|-------------|-----------|---------|
| **Go 1.25+** | Building from source only | [go.dev/dl](https://go.dev/dl/) |
| **Azure PG Flexible Server** | Database hosting | See [Section 4](#azure-postgresql-setup) |
| **Entra ID auth enabled** | Passwordless login | See [Section 4](#azure-postgresql-setup) |
| **Team members added as PG users** | Per-dev access | See [Section 4](#azure-postgresql-setup) |
| **Azure CLI** (`az`) | Alternative auth method (technical users) | Optional — `engram login` is the recommended method |

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

The `-tags pgstore` flag activates the PostgreSQL store backend in addition to SQLite. The resulting `engram` binary auto-selects the backend based on whether a `database-url` is configured.

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

### Recommended: `engram login` (Browser Flow)

`engram login` is the recommended authentication method for all users. It opens a browser, authenticates via the Microsoft Entra ID interactive flow with PKCE, and caches a refresh token locally for up to ~90 days. You authenticate once and don't need to think about it again.

**How it works:**

1. `engram login` opens your default browser (or prints a URL to visit on headless systems)
2. You log in with your organizational Microsoft account
3. engram receives an access token + refresh token and caches them at `~/.engram/token-cache.json`
4. On subsequent runs, engram silently refreshes using the cached refresh token
5. After ~90 days (or if Conditional Access revokes the session), you run `engram login` again

> **Headless / SSH environments:** When no browser is available, `engram login` automatically falls back to the Device Code flow — it prints a URL and a short code. Open the URL on any device, enter the code, and the terminal completes authentication.

### Step-by-Step Setup (for a new developer)

```bash
# 1. Set your database connection string
engram config set database-url "postgres://user@server.postgres.database.azure.com:5432/dbname?sslmode=require"

# 2. Set your Azure App Registration tenant and client IDs
#    (your admin will give you these values)
engram config set tenant-id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
engram config set client-id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

# 3. Authenticate (opens browser, caches token for ~90 days)
engram login

# 4. Configure your AI coding tool (one-time setup)
engram setup opencode    # for OpenCode
# or configure manually — see MCP Client Configuration below

# 5. Restart your AI tool → done
```

Replace:
- `user` — your Entra ID email (e.g., `dev-a@company.com`)
- `server` — your Azure PG server name (e.g., `engram-db`)
- `dbname` — your database name (typically `engram`)

### `engram config set` Reference

The `config set` command writes to `~/.engram/config.yaml` (Windows: `%USERPROFILE%\.engram\config.yaml`). All keys are stored persistently — no need to set them each session.

| Key | Description | Example |
|-----|-------------|---------|
| `database-url` | PostgreSQL connection string | `postgres://user@server.postgres.database.azure.com:5432/dbname?sslmode=require` |
| `tenant-id` | Azure Entra ID tenant UUID | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `client-id` | Azure App Registration client UUID | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `auth-method` | `entra` or `password` | `entra` (default for Azure PG) |

> **Profiles:** `engram config set` supports named profiles for switching between databases. Use `engram config use <profile-name>` to activate a profile.

### Alternative: `az login` (for technical users)

If you already use the Azure CLI for other Azure work, `az login` also works. engram detects Azure CLI credentials automatically via `DefaultAzureCredential`.

```bash
# Install Azure CLI (macOS)
brew install azure-cli

# Install Azure CLI (Linux — Debian/Ubuntu)
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Install Azure CLI (Windows)
winget install Microsoft.AzureCLI

# Authenticate
az login
```

The `az login` refresh token also lasts ~90 days. See [Entra ID Token Lifecycle](#entra-id-token-lifecycle) for how both methods interact.

### 5b. With Password (for local dev/testing)

For local PostgreSQL instances (Docker, Homebrew postgres, etc.) where Entra ID is not needed.

#### macOS / Linux

```bash
# Start a local PG (if you don't have one):
# docker run -d --name engram-db -e POSTGRES_DB=engram -e POSTGRES_USER=engram -e POSTGRES_PASSWORD=password -p 5432:5432 postgres:16-alpine

engram config set database-url "postgres://engram:password@localhost:5432/engram?sslmode=disable"
engram config set auth-method password
```

#### Windows

```powershell
engram config set database-url "postgres://engram:password@localhost:5432/engram?sslmode=disable"
engram config set auth-method password
```

### Environment Variables Reference

Environment variables are supported for CI/CD and container environments. They override `config.yaml` values.

| Variable | Description | Default |
|----------|-------------|---------|
| `ENGRAM_DATABASE_URL` | PostgreSQL connection string (enables PG mode) | — |
| `ENGRAM_AUTH_METHOD` | `entra` or `password` | Auto-detected: `entra` for `*.database.azure.com`, `password` otherwise |
| `ENGRAM_MIGRATE_SOURCE` | Source SQLite DB for migration | `~/.engram/engram.db` |
| `ENGRAM_DATA_DIR` | Data directory (used for default migration source path) | `~/.engram` |
| `ENGRAM_PORT` | HTTP server port | `7437` |
| `ENGRAM_FTS_LANGUAGE` | PostgreSQL text search language config | `english` |

---

## Team Onboarding Quickstart

This section walks through everything needed to get a new team member connected to the shared database.

### Admin Prerequisites (one-time setup — done by your team lead or architect)

Before developers can connect, an admin must:

1. **Create the Azure App Registration** (once per team):
   - Go to Azure Portal → **App registrations** → **New registration**
   - Name it (e.g., `engram-mcp`)
   - Set redirect URI: `http://localhost:3333/callback` (for the browser login flow)
   - Note the **Application (client) ID** and **Directory (tenant) ID**
   - Under **Certificates & secrets** → **Client secrets**, create a secret (for service accounts)
   - Under **API permissions**, ensure `openid`, `profile`, `email`, and `offline_access` are granted

2. **Provision the PostgreSQL database** (see [Azure PostgreSQL Setup](#azure-postgresql-setup))

3. **Add each team member as a PG user** (see [Add Team Members as PG Users](#add-team-members-as-pg-users))

4. **Whitelist each developer's IP** (see [Firewall Rules](#firewall-rules))

5. **Share** the `tenant-id`, `client-id`, and `database-url` with the team (these are not secrets)

### Developer Setup (each developer does this once)

```bash
# Step 1: Install engram
# (see Installation section above for your platform)
engram version    # verify it installed correctly

# Step 2: Configure connection (values provided by your admin)
engram config set database-url "postgres://your-email@server.postgres.database.azure.com:5432/engram?sslmode=require"
engram config set tenant-id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
engram config set client-id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

# Step 3: Authenticate
engram login
# A browser window will open → log in with your corporate Microsoft account
# On headless/SSH: a URL + code will be printed → open in any browser

# Step 4: Set up your AI tool
engram setup opencode    # configures OpenCode automatically
# Restart OpenCode after this step

# Done! Test it:
engram search --query "architecture" --project your-project
```

### Day-to-Day Usage

- **No re-login needed** for ~90 days — the refresh token renews automatically
- After ~90 days (or if your admin revokes access), run `engram login` again — one-time browser flow
- All `mem_save`, `mem_search`, and other MCP tool calls work exactly as in SQLite mode
- Your memories are tagged with your identity (`created_by` = your Entra ID UPN) for audit and filtering

---

## Personal vs Project Scope

Every memory in engram has a **scope** that controls who can see it.

### The Two Scopes

| Scope | Who Can See It | Use For |
|-------|---------------|---------|
| `project` | **All team members** | Architecture decisions, bug fixes, shared conventions, team patterns |
| `personal` | **Only you** (enforced at DB level) | Private notes, sensitive context, work in progress, personal preferences |

### How Scope Works

When saving a memory, specify the scope:

```
mem_save(
  title: "Decided to use Zustand for state",
  scope: "project",     # ← visible to all teammates
  ...
)

mem_save(
  title: "My local DB password for dev",
  scope: "personal",    # ← only you can see this
  ...
)
```

The default scope is `project` — most useful memories are team knowledge.

### How It's Enforced

Scope is enforced at the **database level** using Row Level Security (RLS) with the `engram.identity` GUC (PostgreSQL configuration variable). When engram connects, it sets `SET engram.identity = 'your-email@company.com'` for the session. The RLS policy on the `observations` table then:

- Returns `project` rows to everyone
- Returns `personal` rows ONLY when `current_setting('engram.identity')` matches `created_by`

This means personal memories are private even if two developers use the same database user — your identity is your Entra ID UPN, not the PG role.

### Promoting Personal to Project

If you saved something as `personal` and later want to share it with the team:

```
mem_promote(id: 1234)
```

> **This is irreversible.** Once promoted to `project` scope, the observation is visible to all team members and cannot be moved back to `personal`.

`mem_promote` validates that you are the creator of the observation before allowing the promotion.

### `created_by` Field

Every observation is stamped with `created_by` = your Entra ID UPN (email) at write time. This enables:

- Filtering your own memories: `mem_search(user: "you@company.com")`
- Seeing who saved what: `mem_who` lists all contributors with activity stats
- Audit trail: every record is traceable to a specific person

---

## Team Collaboration Tools

These MCP tools are available to all agents when connected to a PostgreSQL-backed engram instance.

### `mem_projects`

List all projects with statistics (observation count, unique contributors, last activity date).

```
mem_projects()
```

Returns: project name, observation count, contributor count, last activity timestamp. Useful for getting an overview of what your team has been working on.

### `mem_promote`

Promote a personal observation to project scope, making it visible to all team members.

```
mem_promote(id: 1234)
```

**Important:**
- This is **irreversible** — once promoted, the observation stays project-visible
- Only the original creator can promote their own observations
- The tool validates ownership before allowing the promotion

### `mem_who`

List contributors with activity stats — who is using engram on your team, how many observations they've saved, and when they were last active.

```
mem_who()
mem_who(project: "my-project")    # filter to a specific project
```

Returns: contributor identity (Entra UPN), observation count, prompt count, last active date, top observation types.

### `mem_search` — Extended Filters

`mem_search` now supports two additional filter parameters for team collaboration:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `user` | string | Filter by creator identity (email/UPN) | `"dev-a@company.com"` |
| `since` | string | Filter by recency | `"today"`, `"yesterday"`, `"week"`, `"month"`, or ISO date `"2026-01-15"` |

Examples:

```
# Find everything saved by a specific teammate
mem_search(query: "authentication", user: "alice@company.com")

# Find recent memories from this week
mem_search(query: "database", since: "week")

# Combine filters
mem_search(query: "architecture", user: "alice@company.com", since: "month", project: "my-project")
```

---

## MCP Client Configuration

Configure your AI agent to use `engram` directly. The `engram config set` system makes wrapper scripts unnecessary — credentials and connection settings are stored in `~/.engram/config.yaml`.

### `engram setup opencode`

The fastest way to configure OpenCode. Run once after `engram login`:

```bash
engram setup opencode
```

This command:
1. Locates your OpenCode config file automatically (macOS/Linux: `~/.config/opencode/opencode.json`, Windows: `%APPDATA%\opencode\opencode.json`)
2. Adds or updates the `engram` MCP entry
3. Adds the Engram memory protocol to your OpenCode agent prompt

Restart OpenCode after running this command.

### Manual Configuration

If you prefer to configure manually, or use a different AI tool:

#### OpenCode

Edit `~/.config/opencode/opencode.json` (Windows: `%APPDATA%\opencode\opencode.json`):

```json
{
  "mcp": {
    "engram": {
      "type": "local",
      "command": ["engram", "mcp", "--tools=agent"],
      "enabled": true
    }
  }
}
```

> **Note:** The `--tools=agent` flag is optional but recommended — it hides admin-only tools (`mem_merge_projects`) from the agent's tool list.

> **Note for desktop OpenCode users:** The OpenCode desktop app does NOT support TUI plugins. If you use a plugin for authentication (`/engram-login`), run it from a terminal, not from within the desktop app.

#### Claude Code

**Option A: Via `claude mcp add`:**

```bash
claude mcp add engram -- engram mcp
```

**Option B: Manual config** — add to `.claude/settings.json` (project) or `~/.claude/settings.json` (global):

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["mcp"]
    }
  }
}
```

#### Gemini CLI

Edit `~/.gemini/settings.json` (Windows: `%APPDATA%\gemini\settings.json`):

```json
{
  "mcpServers": {
    "engram": {
      "command": "engram",
      "args": ["mcp"]
    }
  }
}
```

#### VS Code (Copilot / Claude Code Extension)

Add to `.vscode/mcp.json` (workspace) or user-level `mcp.json`:

```json
{
  "servers": {
    "engram": {
      "command": "engram",
      "args": ["mcp"]
    }
  }
}
```

#### Codex

Edit `~/.codex/config.toml` (Windows: `%APPDATA%\codex\config.toml`):

```toml
[mcp_servers.engram]
command = "engram"
args = ["mcp"]
```

#### Any Other MCP Agent

The pattern is always the same — use `engram mcp` in your agent's MCP config. engram reads its connection config from `~/.engram/config.yaml` automatically. The `mcp` subcommand starts the MCP server on stdio, identical to the SQLite version.

### Config Profiles (advanced)

If you need to switch between multiple databases (e.g., dev vs. production), use named profiles:

```bash
# Save current config as a named profile
engram config save-profile prod

# Create a dev profile
engram config set database-url "postgres://user@dev-server.postgres.database.azure.com:5432/engram?sslmode=require"
engram config save-profile dev

# Switch between them
engram config use prod
engram config use dev
```

---

## Migration from SQLite

Migrate your existing local engram memories to the shared PostgreSQL database. The migration is **idempotent** — safe to re-run.

### macOS / Linux

```bash
# Set source (your existing engram DB — defaults to ~/.engram/engram.db)
export ENGRAM_MIGRATE_SOURCE="$HOME/.engram/engram.db"

# Authenticate with Azure (if not already logged in via engram login or az login)
engram login

# Run migration
engram migrate

# Verify
engram search --query "test" --project <your-project>
```

### Windows

```powershell
# Set source
$env:ENGRAM_MIGRATE_SOURCE = "$env:USERPROFILE\.engram\engram.db"

# Authenticate
engram login

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

### Two Authentication Paths

engram supports two ways to authenticate with Entra ID. Both result in the same PostgreSQL connection experience:

```
PATH A: engram login (recommended)
─────────────────────────────────
  engram login
       │
       ├─ Desktop machine?
       │    └─ Opens browser → PKCE interactive flow
       │         └─ Receives access token + refresh token
       │              └─ Cached at ~/.engram/token-cache.json
       │
       └─ Headless/SSH?
            └─ Prints Device Code URL → user authenticates on any browser
                 └─ Receives access token + refresh token
                      └─ Cached at ~/.engram/token-cache.json

  On subsequent runs:
       └─ token-cache.json found
            ├─ Refresh token still valid (~90 days)?
            │    └─ Silent refresh → new access token
            └─ Refresh token expired?
                 └─ Prompt user to run engram login again


PATH B: az login (alternative for Azure CLI users)
───────────────────────────────────────────────────
  az login (one-time)
       │
       ▼
  Azure CLI caches credentials locally
       │
       ▼
  engram starts → DefaultAzureCredential
       │         (checks in order:)
       │         1. Azure CLI cache
       │         2. Managed Identity
       │         3. Environment Variables
       │         4. Visual Studio Code
       │
       ▼
  Token acquired → used as PG password
```

### Credential Chain (how engram decides which path to use)

When engram needs a token, it tries in this order:

1. **`DefaultAzureCredential`** — checks Azure CLI, Managed Identity, env vars, VS Code (in that order)
2. **`token-cache.json` fallback** — if DefaultAzureCredential fails, engram tries the cached refresh token from `engram login`

If both fail, engram prompts you to run `engram login`.

### Token Refresh Flow (both paths)

```
  MCP tool call (e.g., mem_save)
       │
       ▼
  pgx pool needs a connection
       │
       ▼
  BeforeConnect hook fires → TokenProvider.Token()
       │
       ├── Token valid? (>5 min to expiry)
       │    └─ Use cached token
       │
       └── Token expired or within 5 min of expiry?
            └─ Acquire new token
                 ├─ PATH A: Silent refresh via token-cache.json
                 └─ PATH B: Azure CLI refresh
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

- **Access token validity:** Azure AD tokens last ~60-90 minutes
- **Refresh threshold:** engram refreshes tokens **5 minutes before expiry** — no interruptions during normal use
- **Refresh token validity:** ~90 days (unless Conditional Access policy shortens this)
- **Caching:** All connections in the pool share the same cached token — only one Azure AD request per refresh cycle
- **Thread safety:** Multiple concurrent MCP tool calls safely share the token (protected by sync.RWMutex with double-check locking)
- **Connection pool:** Max 5 connections, max lifetime 30 minutes (rotated before token expiry)

### When Does `engram login` / `az login` Expire?

- Refresh tokens last **~90 days** (or until admin revokes them, or Conditional Access policy intervenes)
- Engram silently renews access tokens using the refresh token — you won't be interrupted
- If the refresh token expires, run `engram login` again — one browser login flow
- CI/CD: Use managed identity or service principal env vars (`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`) instead of `engram login`

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

**Cause:** No valid credentials found — neither `engram login` token-cache nor `az login` session is present.

**Fix:**
```bash
engram login
# Then retry your engram command
```

Or, if you prefer the Azure CLI path:
```bash
az login
```

If you're using password auth and see `password authentication failed`:
- Verify the password in your `database-url` config is correct
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

### Windows: Browser URL Truncation

**Symptom:** The browser opens but shows an error, or the `engram login` URL is cut off in the terminal.

**Cause:** Windows Command Prompt and older PowerShell versions truncate long URLs printed to the terminal.

**Fix:**
- Use **Windows Terminal** or **PowerShell 7+** (pwsh) — they handle long URLs correctly
- If the URL is truncated, copy it to a browser manually: look for the `https://login.microsoftonline.com/...` portion and paste into your browser
- Alternatively, the Device Code fallback avoids the URL issue entirely — use a short URL + code instead

### Windows: PATH Issues

**Symptom:** MCP client can't find `engram`.

**Fix:**
- Ensure the directory containing `engram.exe` is in your system `PATH`
- In MCP configs, use the **full path** if PATH resolution fails:
  ```json
  {
    "command": "C:\\Users\\<user>\\go\\bin\\engram.exe",
    "args": ["mcp"]
  }
  ```

### `AADSTS900144: The request body must contain the following parameter: 'scope'`

**Cause:** The `client-id` is configured but `tenant-id` is missing, or the App Registration does not have the required scopes (`openid`, `profile`, `email`, `offline_access`).

**Fix:**
1. Verify both are set:
   ```bash
   engram config set tenant-id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
   engram config set client-id "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
   ```
2. Ask your Azure admin to confirm the App Registration has `openid`, `profile`, `email`, and `offline_access` API permissions granted

### `tenant-id` or `client-id` Not Recognized

**Symptom:** `engram config set tenant-id ...` returns `unknown key`.

**Cause:** Older engram builds had a restricted `ValidKeys` list that didn't include `tenant-id` and `client-id`.

**Fix:** Upgrade to the latest engram release — these keys were added to the config system.

### OpenCode Desktop App: Plugin Not Working

**Symptom:** `/engram-login` command does nothing or shows an error in the OpenCode desktop app.

**Cause:** The OpenCode **desktop app** does NOT support TUI plugins. Plugin commands like `/engram-login` require a terminal environment.

**Fix:**
- Run `engram login` directly from your terminal (not from within OpenCode)
- Use `engram setup opencode` from the terminal to configure OpenCode automatically
- The OpenCode **terminal mode** (`opencode` CLI) does support plugins

### ENGRAM_DATABASE_URL Must Be Set

```
engram: ENGRAM_DATABASE_URL must be set for PostgreSQL mode
```

**Cause:** engram requires a connection string for PostgreSQL mode, and neither `config.yaml` nor the environment variable is set.

**Fix:** Run `engram config set database-url "..."` (see [Authentication Configuration](#authentication-configuration)), or set `ENGRAM_DATABASE_URL` in your environment.

### Linux: Azure CLI Installation Variants

Different package managers install `az` to different paths. If `az login` gives "command not found":

| Distro | Install | Path |
|--------|---------|------|
| Debian/Ubuntu | `curl -sL https://aka.ms/InstallAzureCLIDeb \| sudo bash` | `/usr/bin/az` |
| RHEL/Fedora | `sudo dnf install azure-cli` | `/usr/bin/az` |
| Snap | `sudo snap install azure-cli --classic` | `/snap/bin/az` |
| pip | `pip install azure-cli` | `~/.local/bin/az` |

Ensure the installed path is in your `PATH`. Note: `engram login` does NOT require Azure CLI — it's a self-contained browser/device-code flow.

---

## Rollback to SQLite

If you need to switch back to the standard SQLite-based engram:

### 1. Change MCP Config

Remove the database config — use `engram` directly in your agent's MCP config without a `database-url`:

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

You can also unset the database-url from config:

```bash
engram config unset database-url
```

### 2. Your Local Data Is Untouched

The SQLite database at `~/.engram/engram.db` (Windows: `%USERPROFILE%\.engram\engram.db`) was **never modified** by the PG migration or by running engram in PG mode. It's still there with all your pre-migration data.

### 3. Export from PG (Optional)

If you accumulated new data in PostgreSQL that you want to keep locally:

```bash
# Export from PG to JSON (with database-url configured)
engram export engram-backup.json

# Import into local SQLite (after unsetting database-url)
engram import engram-backup.json
```

### 4. Verify

```bash
engram search --query "test" --project <your-project>
engram stats
```

You should see your original local memories plus any imported from PG.

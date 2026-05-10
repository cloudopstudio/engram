# Engram for Pi

Engram for Pi gives the Pi coding agent persistent memory that survives sessions and compactions.

It installs a Pi extension for session capture and configures Engram MCP tools through `pi-mcp-adapter`.

## Quick Start

```bash
pi install npm:@gentleman-programming/pi-engram
pi install npm:pi-mcp-adapter
pi-engram init
```

Restart Pi after installation, then ask Pi to recall previous work or call `mem_context`.

## Requirements

- Pi coding agent with npm package support.
- Engram installed as `engram` on `PATH`, or `ENGRAM_BIN` pointing at the binary.
- `pi-mcp-adapter` for direct `mem_*` MCP tools.

If you only want HTTP session capture against an already running Engram server, set `ENGRAM_URL` and the extension will not auto-start a local `engram serve` process.

## What It Adds

- **Session memory**: Pi prompts, session summaries, and passive task learnings are sent to `engram serve`.
- **MCP tools**: `mem_search`, `mem_save`, `mem_context`, and the other Engram tools are exposed through `pi-mcp-adapter` and `engram mcp`.
- **Safe startup**: missing `engram` binaries degrade cleanly instead of crashing Pi with `spawn engram ENOENT`.

## Configuration

### Existing Engram server

Use an already running Engram HTTP server:

```bash
ENGRAM_URL=http://127.0.0.1:7437 pi
```

When `ENGRAM_URL` is set, the extension treats the server as externally managed and does not auto-start `engram serve`.

### Custom Engram binary

Use a custom Engram binary for MCP tools and local auto-start:

```bash
ENGRAM_BIN=/path/to/engram pi
```

If the binary is missing, Pi keeps running and memory degrades instead of crashing with `spawn engram ENOENT`.

## Install Command Details

`pi-engram init` writes Pi-owned config in the Pi agent directory:

- `settings.json`: ensures `npm:pi-mcp-adapter` and `npm:@gentleman-programming/pi-engram` are declared.
- `mcp.json`: adds an `engram` MCP server that launches `engram mcp --tools=agent` through a safe Node wrapper.

Existing `mcpServers.engram` entries are preserved unless you pass `--force`:

```bash
pi-engram init --force
```

The command respects `PI_CODING_AGENT_DIR`; otherwise it writes to `~/.pi/agent`.

## Mental Model

```text
Pi events -> pi-engram extension -> ENGRAM_URL / engram serve -> SQLite
Pi MCP tools -> pi-mcp-adapter -> ENGRAM_BIN / engram mcp -> SQLite
```

HTTP event capture and MCP tools are separate paths. Engram currently exposes MCP over stdio, so direct MCP tools still need an Engram binary even when `ENGRAM_URL` points at a remote HTTP server.

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `mem_*` tools are missing | Install `pi-mcp-adapter`, run `pi-engram init`, then restart Pi. |
| Pi cannot find `engram` | Set `ENGRAM_BIN=/absolute/path/to/engram`. |
| Session capture should use another server | Set `ENGRAM_URL=http://host:7437`. |
| Existing MCP config was not replaced | Run `pi-engram init --force`. |

## Next Steps

- Run `engram tui` to inspect stored memories.
- Use `mem_current_project` to confirm project detection before writing memories.
- See the main Engram setup guide: <https://github.com/Gentleman-Programming/engram/blob/main/docs/AGENT-SETUP.md>

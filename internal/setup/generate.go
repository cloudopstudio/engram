package setup

// Sync embedded plugin copies from the source of truth (plugin/ directory).
// Only OpenCode needs embedding — Claude Code is installed via marketplace.
// Run: go generate ./internal/setup/
//go:generate sh -c "rm -rf plugins/opencode && mkdir -p plugins/opencode && cp ../../plugin/opencode/engram.ts plugins/opencode/"
//go:generate sh -c "rm -rf plugins/opencode-azure-entra-auth && mkdir -p plugins/opencode-azure-entra-auth && cp ../../plugins/opencode-azure-entra-auth/index.ts ../../plugins/opencode-azure-entra-auth/tui.tsx ../../plugins/opencode-azure-entra-auth/package.json ../../plugins/opencode-azure-entra-auth/README.md plugins/opencode-azure-entra-auth/"

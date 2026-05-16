package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Gentleman-Programming/engram/internal/store"
)

// cmdLogin authenticates the user with Azure and caches the token for future
// MCP sessions. On desktop environments the browser is opened automatically
// (Interactive Browser Flow with PKCE). On headless environments (SSH,
// containers) the Device Code Flow is used as a fallback.
//
// Usage:
//
//	engram login                         # uses default profile
//	engram login --profile arquitectura  # uses specific profile
func cmdLogin(cfg store.Config) {
	requirePostgresBackend(cfg, "login")

	tenantID, clientID, err := store.ResolveInteractiveAuthExported(cfg.DataDir, cfg.Profile)
	if err != nil {
		fatal(err)
	}

	if store.IsHeadlessExported() {
		fmt.Fprintf(os.Stderr, "engram: headless environment detected — using device code flow...\n")
	} else {
		fmt.Fprintf(os.Stderr, "engram: opening browser for Azure authentication...\n")
	}

	tp, err := store.NewDeviceCodeTokenProvider(tenantID, clientID, cfg.DataDir)
	if err != nil {
		fatal(err)
	}

	// Force token acquisition — this triggers the auth prompt if not already cached.
	if _, err := tp.Token(context.Background()); err != nil {
		fatal(fmt.Errorf("authentication failed: %w", err))
	}

	identity := tp.Identity()
	if identity != "" {
		fmt.Fprintf(os.Stderr, "\n  Authentication successful!\n  Authenticated as %s\n  Token cached (~90 days).\n\n", identity)
	} else {
		fmt.Fprintf(os.Stderr, "\n  Authentication successful!\n  Token cached (~90 days).\n\n")
	}
}

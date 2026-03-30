//go:build pgstore

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Gentleman-Programming/engram/internal/store"
)

// cmdLogin runs the Azure Device Code Flow interactively, caches the token,
// and exits. This lets users pre-authenticate before starting an MCP session.
//
// Usage:
//
//	engram login                         # uses default profile
//	engram login --profile arquitectura  # uses specific profile
func cmdLogin(cfg store.Config) {
	tenantID, clientID, err := store.ResolveInteractiveAuthExported(cfg.DataDir, cfg.Profile)
	if err != nil {
		fatal(err)
	}

	fmt.Fprintf(os.Stderr, "engram: authenticating with Azure (device code flow)...\n")

	tp, err := store.NewDeviceCodeTokenProvider(tenantID, clientID)
	if err != nil {
		fatal(err)
	}

	// Force token acquisition — this triggers the device code prompt.
	if _, err := tp.Token(context.Background()); err != nil {
		fatal(fmt.Errorf("authentication failed: %w", err))
	}

	identity := tp.Identity()
	if identity != "" {
		fmt.Fprintf(os.Stderr, "Authenticated successfully as %s. Token cached.\n", identity)
	} else {
		fmt.Fprintf(os.Stderr, "Authenticated successfully. Token cached.\n")
	}
}

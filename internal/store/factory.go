// Package store: New is the unified factory that picks a backend (SQLite or
// PostgreSQL) based on Config.DBType. When DBType is empty, the factory
// auto-detects: ENGRAM_DATABASE_URL or a configured `database-url` profile
// key promotes the backend to PostgreSQL; otherwise it falls back to SQLite.
//
// This file lives without build tags so a single binary compiles in both
// backends. Callers (CLI, tests, MCP, HTTP, TUI) keep using store.New(cfg)
// exactly as before — the dispatch is invisible to them.
package store

import (
	"fmt"
	"os"

	"github.com/Gentleman-Programming/engram/internal/config"
)

// New is the unified Store factory. It dispatches to NewSQLiteStore or
// NewPostgresStore based on cfg.DBType, performing auto-detection when
// DBType is empty.
func New(cfg Config) (Store, error) {
	switch cfg.DBType {
	case DBTypePostgres:
		return NewPostgresStore(cfg)
	case DBTypeSQLite:
		return NewSQLiteStore(cfg)
	case "":
		// Auto-detect: if a PostgreSQL connection string is configured
		// (env var or profile key), prefer PostgreSQL; otherwise fall
		// back to SQLite. This keeps the historical zero-config UX:
		// users who never set ENGRAM_DATABASE_URL keep getting SQLite.
		if os.Getenv("ENGRAM_DATABASE_URL") != "" {
			return NewPostgresStore(cfg)
		}
		if v, err := config.GetWithProfile(cfg.DataDir, cfg.Profile, "database-url"); err == nil && v != "" {
			return NewPostgresStore(cfg)
		}
		return NewSQLiteStore(cfg)
	default:
		return nil, fmt.Errorf("store: unknown db type %q (valid: sqlite, postgres)", cfg.DBType)
	}
}

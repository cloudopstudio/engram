package main

import (
	"fmt"
	"os"

	"github.com/Gentleman-Programming/engram/internal/config"
	"github.com/Gentleman-Programming/engram/internal/store"
)

// requirePostgresBackend ensures the command runs only against a PostgreSQL
// backend. The 3 PG-only commands (login, aws-login, migrate) use this guard
// to fail early with a clear error message when the resolved backend would
// be SQLite.
//
// Resolution mirrors store.New auto-detect:
//   - explicit DBType=postgres → OK
//   - explicit DBType=sqlite   → reject
//   - empty DBType + ENGRAM_DATABASE_URL set → OK
//   - empty DBType + database-url in profile/config → OK
//   - otherwise → reject
func requirePostgresBackend(cfg store.Config, cmdName string) {
	if cfg.DBType == store.DBTypePostgres {
		return
	}
	if cfg.DBType == store.DBTypeSQLite {
		fmt.Fprintf(os.Stderr, "engram: '%s' requires PostgreSQL backend, but --db-type/ENGRAM_DB_TYPE is set to sqlite.\n", cmdName)
		fmt.Fprintln(os.Stderr, "Set --db-type=postgres, ENGRAM_DB_TYPE=postgres, or define ENGRAM_DATABASE_URL.")
		exitFunc(1)
		return
	}
	// cfg.DBType == "" → auto-detect, same logic as store.New.
	if os.Getenv("ENGRAM_DATABASE_URL") != "" {
		return
	}
	if v, err := config.GetWithProfile(cfg.DataDir, cfg.Profile, "database-url"); err == nil && v != "" {
		return
	}
	fmt.Fprintf(os.Stderr, "engram: '%s' requires PostgreSQL backend.\n", cmdName)
	fmt.Fprintln(os.Stderr, "Set --db-type=postgres, ENGRAM_DB_TYPE=postgres, or define ENGRAM_DATABASE_URL.")
	exitFunc(1)
}

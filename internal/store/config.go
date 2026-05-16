// Package store: shared Config and its constructors. Lives in a file without
// build tags so the SQLite and PostgreSQL backends share a single Config
// definition.
package store

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	DataDir              string
	Profile              string // Active profile name (from --profile flag or default-profile)
	AuthInteractive      bool   // Enable device code flow for Azure auth
	MaxObservationLength int
	MaxContextResults    int
	MaxSearchResults     int
	DedupeWindow         time.Duration
	DBType               DBType // Backend selector; empty = auto-detect (see factory.go)
}

// DBType selects the storage backend. An empty DBType triggers auto-detection
// by the New factory (see factory.go): ENGRAM_DATABASE_URL or a configured
// database-url profile key promotes the backend to PostgreSQL; otherwise the
// SQLite backend is used.
type DBType string

const (
	DBTypeSQLite   DBType = "sqlite"
	DBTypePostgres DBType = "postgres"
)

func DefaultConfig() (Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Config{}, fmt.Errorf("engram: determine home directory: %w", err)
	}
	return Config{
		DataDir:              filepath.Join(home, ".engram"),
		MaxObservationLength: 50000,
		MaxContextResults:    20,
		MaxSearchResults:     20,
		DedupeWindow:         15 * time.Minute,
	}, nil
}

// FallbackConfig returns a Config with the given DataDir and default values.
// Use this when DefaultConfig fails and you have resolved the home directory
// through alternative means.
func FallbackConfig(dataDir string) Config {
	return Config{
		DataDir:              dataDir,
		MaxObservationLength: 50000,
		MaxContextResults:    20,
		MaxSearchResults:     20,
		DedupeWindow:         15 * time.Minute,
	}
}

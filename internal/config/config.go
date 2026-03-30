// Package config provides a flat key-value configuration store for engram.
// It reads/writes {DataDir}/config.json with atomic writes and 0600 permissions.
// The config package is build-tag-agnostic — it works identically for SQLite and PG builds.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// KeyInfo describes a valid configuration key.
type KeyInfo struct {
	EnvVar      string // mapped environment variable (empty = no env mapping)
	Default     string // default value (empty = no default, key is optional)
	Description string // human-readable description for help/list output
}

// ValidKeys is the exhaustive set of allowed configuration keys.
var ValidKeys = map[string]KeyInfo{
	"database-url": {
		EnvVar:      "ENGRAM_DATABASE_URL",
		Default:     "",
		Description: "PostgreSQL connection string",
	},
	"auth-method": {
		EnvVar:      "ENGRAM_AUTH_METHOD",
		Default:     "",
		Description: "Authentication method: entra or password (auto-detected if unset)",
	},
	"server-port": {
		EnvVar:      "ENGRAM_PORT",
		Default:     "7437",
		Description: "HTTP server port",
	},
	"default-project": {
		EnvVar:      "",
		Default:     "",
		Description: "Default project name for commands",
	},
}

// ConfigFileName is the name of the config file within the data directory.
const ConfigFileName = "config.json"

// Path returns the absolute path to the config file for a given data directory.
func Path(dataDir string) string {
	return filepath.Join(dataDir, ConfigFileName)
}

// SortedKeys returns valid key names in alphabetical order.
func SortedKeys() []string {
	keys := make([]string, 0, len(ValidKeys))
	for k := range ValidKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ValidKeyList returns a sorted, comma-separated list of valid keys.
func ValidKeyList() string {
	return strings.Join(SortedKeys(), ", ")
}

// Load reads the config file and returns the key-value map.
// Returns an empty map (not error) if the file does not exist.
// Returns an error if the file exists but contains invalid JSON.
func Load(dataDir string) (map[string]string, error) {
	path := Path(dataDir)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return make(map[string]string), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg map[string]string
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: file is corrupt — delete it or fix the JSON: %w", path, err)
	}
	if cfg == nil {
		cfg = make(map[string]string)
	}
	return cfg, nil
}

// Save writes the config map to disk with atomic write and 0600 permissions.
// Creates the data directory if it doesn't exist.
func Save(dataDir string, cfg map[string]string) error {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	data = append(data, '\n')

	// Atomic write: temp file in same directory, then rename.
	path := Path(dataDir)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("write config temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp) // cleanup on rename failure
		return fmt.Errorf("rename config: %w", err)
	}
	return nil
}

// Get returns the value for a config key from the config file.
// Returns ("", nil) if the key is valid but not set in the config file.
// Returns error if the key is invalid or the config file is corrupt.
func Get(dataDir, key string) (string, error) {
	if _, ok := ValidKeys[key]; !ok {
		return "", fmt.Errorf("unknown config key %q. Valid keys: %s", key, ValidKeyList())
	}
	cfg, err := Load(dataDir)
	if err != nil {
		return "", err
	}
	return cfg[key], nil
}

// Set validates the key, then writes the value to the config file.
// Creates the file if it doesn't exist.
func Set(dataDir, key, value string) error {
	if _, ok := ValidKeys[key]; !ok {
		return fmt.Errorf("unknown config key %q. Valid keys: %s", key, ValidKeyList())
	}
	cfg, err := Load(dataDir)
	if err != nil {
		return err
	}
	cfg[key] = value
	return Save(dataDir, cfg)
}

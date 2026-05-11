// Package config provides a key-value configuration store for engram with
// profile support. It reads/writes {DataDir}/config.json with atomic writes
// and 0600 permissions.
//
// The config file supports an optional "profiles" map for multi-database
// configurations. Values are resolved with the priority:
//
//	env var > profile value > root value > default
//
// The config package is build-tag-agnostic — it works identically for
// SQLite and PG builds.
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
		Description: "Authentication method: entra, aws-iam, or password (auto-detected if unset)",
	},
	"aws-region": {
		EnvVar:      "AWS_REGION",
		Default:     "",
		Description: "AWS region for RDS IAM authentication (e.g. us-east-1)",
	},
	"aws-profile": {
		EnvVar:      "AWS_PROFILE",
		Default:     "",
		Description: "AWS shared-config profile name for RDS IAM authentication (used by aws sso login)",
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
	"default-profile": {
		EnvVar:      "",
		Default:     "",
		Description: "Default profile name (used when --profile is not given)",
	},
	"tenant-id": {
		EnvVar:      "AZURE_TENANT_ID",
		Default:     "",
		Description: "Azure Entra ID tenant ID for database authentication",
	},
	"client-id": {
		EnvVar:      "AZURE_CLIENT_ID",
		Default:     "",
		Description: "Azure App Registration client ID for database authentication",
	},
}

// profileExcludedKeys are keys that cannot be set inside a profile.
// These are global/personal preferences, not per-database settings.
var profileExcludedKeys = map[string]bool{
	"default-project": true,
	"default-profile": true,
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

// ProfileKeys returns the valid keys that can be set inside a profile,
// sorted alphabetically.
func ProfileKeys() []string {
	var keys []string
	for k := range ValidKeys {
		if !profileExcludedKeys[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// ValidKeyList returns a sorted, comma-separated list of valid keys.
func ValidKeyList() string {
	return strings.Join(SortedKeys(), ", ")
}

// ─── Rich config structure ───────────────────────────────────────────────────

// configFile is the internal representation of config.json.
// It supports both the flat map (backward compatible) and the profiles map.
type configFile struct {
	// Root-level key-value pairs.
	Root map[string]string `json:"-"`
	// Profile-specific overrides. Key = profile name, value = key-value map.
	Profiles map[string]map[string]string `json:"-"`
}

// loadRaw reads the config file and returns the full configFile structure.
func loadRaw(dataDir string) (*configFile, error) {
	path := Path(dataDir)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &configFile{
			Root:     make(map[string]string),
			Profiles: make(map[string]map[string]string),
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Parse into a generic map to handle both flat and structured formats.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse %s: file is corrupt — delete it or fix the JSON: %w", path, err)
	}
	if raw == nil {
		return &configFile{
			Root:     make(map[string]string),
			Profiles: make(map[string]map[string]string),
		}, nil
	}

	cf := &configFile{
		Root:     make(map[string]string),
		Profiles: make(map[string]map[string]string),
	}

	for k, v := range raw {
		if k == "profiles" {
			if err := json.Unmarshal(v, &cf.Profiles); err != nil {
				return nil, fmt.Errorf("parse %s profiles: %w", path, err)
			}
			continue
		}
		// Root-level string value.
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			// Skip non-string values at root level gracefully.
			continue
		}
		cf.Root[k] = s
	}

	if cf.Profiles == nil {
		cf.Profiles = make(map[string]map[string]string)
	}

	return cf, nil
}

// saveRaw writes the configFile structure to disk.
func saveRaw(dataDir string, cf *configFile) error {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	// Build ordered output map for clean JSON.
	out := make(map[string]any)
	for k, v := range cf.Root {
		out[k] = v
	}
	if len(cf.Profiles) > 0 {
		out["profiles"] = cf.Profiles
	}

	data, err := json.MarshalIndent(out, "", "  ")
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

// ─── Public API (backward compatible) ────────────────────────────────────────

// Load reads the config file and returns the root-level key-value map.
// Returns an empty map (not error) if the file does not exist.
// Returns an error if the file exists but contains invalid JSON.
// This returns ONLY root-level values (no profiles), preserving backward
// compatibility with existing callers.
func Load(dataDir string) (map[string]string, error) {
	cf, err := loadRaw(dataDir)
	if err != nil {
		return nil, err
	}
	return cf.Root, nil
}

// Save writes the root-level config map to disk with atomic write and 0600
// permissions. Creates the data directory if it doesn't exist.
// Preserves any existing profiles section.
func Save(dataDir string, cfg map[string]string) error {
	// Load existing to preserve profiles.
	cf, err := loadRaw(dataDir)
	if err != nil {
		// If load fails, start fresh.
		cf = &configFile{
			Root:     make(map[string]string),
			Profiles: make(map[string]map[string]string),
		}
	}
	cf.Root = cfg
	return saveRaw(dataDir, cf)
}

// Get returns the value for a config key from the root-level config file.
// Returns ("", nil) if the key is valid but not set in the config file.
// Returns error if the key is invalid or the config file is corrupt.
func Get(dataDir, key string) (string, error) {
	if _, ok := ValidKeys[key]; !ok {
		return "", fmt.Errorf("unknown config key %q. Valid keys: %s", key, ValidKeyList())
	}
	cf, err := loadRaw(dataDir)
	if err != nil {
		return "", err
	}
	return cf.Root[key], nil
}

// Set validates the key, then writes the value to the root-level config file.
// Creates the file if it doesn't exist.
func Set(dataDir, key, value string) error {
	if _, ok := ValidKeys[key]; !ok {
		return fmt.Errorf("unknown config key %q. Valid keys: %s", key, ValidKeyList())
	}
	cf, err := loadRaw(dataDir)
	if err != nil {
		return err
	}
	cf.Root[key] = value
	return saveRaw(dataDir, cf)
}

// ─── Profile API ─────────────────────────────────────────────────────────────

// GetWithProfile resolves a config value with profile fallback:
//
//	profile value (if profile is non-empty) → root value → ""
//
// Environment variables are NOT checked here — callers handle env priority.
// Returns error if the key is invalid or the config file is corrupt.
func GetWithProfile(dataDir, profile, key string) (string, error) {
	if _, ok := ValidKeys[key]; !ok {
		return "", fmt.Errorf("unknown config key %q. Valid keys: %s", key, ValidKeyList())
	}
	cf, err := loadRaw(dataDir)
	if err != nil {
		return "", err
	}

	// If no explicit profile, check default-profile.
	if profile == "" {
		profile = cf.Root["default-profile"]
	}

	// Try profile-specific value first.
	if profile != "" {
		if pCfg, ok := cf.Profiles[profile]; ok {
			if v, ok := pCfg[key]; ok && v != "" {
				return v, nil
			}
		}
	}

	// Fall back to root.
	return cf.Root[key], nil
}

// SetWithProfile sets a key-value pair inside a named profile.
// Creates the profile if it doesn't exist.
// Returns error if the key is invalid, excluded from profiles, or the
// config file is corrupt.
func SetWithProfile(dataDir, profile, key, value string) error {
	if profile == "" {
		return fmt.Errorf("profile name cannot be empty")
	}
	if _, ok := ValidKeys[key]; !ok {
		return fmt.Errorf("unknown config key %q. Valid keys: %s", key, ValidKeyList())
	}
	if profileExcludedKeys[key] {
		return fmt.Errorf("key %q cannot be set inside a profile (it's a global setting)", key)
	}

	cf, err := loadRaw(dataDir)
	if err != nil {
		return err
	}

	if cf.Profiles[profile] == nil {
		cf.Profiles[profile] = make(map[string]string)
	}
	cf.Profiles[profile][key] = value
	return saveRaw(dataDir, cf)
}

// ListProfiles returns the names of all configured profiles, sorted
// alphabetically. Returns an empty slice if no profiles exist.
func ListProfiles(dataDir string) ([]string, error) {
	cf, err := loadRaw(dataDir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(cf.Profiles))
	for name := range cf.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// GetProfileConfig returns the key-value map for a specific profile.
// Returns nil if the profile doesn't exist.
func GetProfileConfig(dataDir, profile string) (map[string]string, error) {
	cf, err := loadRaw(dataDir)
	if err != nil {
		return nil, err
	}
	return cf.Profiles[profile], nil
}

// ResolveProfile determines the effective profile name given an explicit
// --profile flag value and the config file's default-profile setting.
// Returns "" if no profile is active.
func ResolveProfile(dataDir, explicit string) string {
	if explicit != "" {
		return explicit
	}
	// Check config for default-profile (ignore errors — return "").
	cf, err := loadRaw(dataDir)
	if err != nil {
		return ""
	}
	return cf.Root["default-profile"]
}

// ValidateProfile checks whether a named profile exists in the config file.
// Returns true if the profile exists (has at least one key), false otherwise.
// Returns false on any error (graceful — callers use this for warnings only).
func ValidateProfile(dataDir, profile string) bool {
	if profile == "" {
		return true // no profile to validate
	}
	cf, err := loadRaw(dataDir)
	if err != nil {
		return false
	}
	_, ok := cf.Profiles[profile]
	return ok
}

// DeleteProfile removes a named profile from config.json. Returns an error
// if the profile does not exist. If the deleted profile is the current
// default-profile, default-profile is cleared.
func DeleteProfile(dataDir, profile string) error {
	if profile == "" {
		return fmt.Errorf("profile name cannot be empty")
	}

	cf, err := loadRaw(dataDir)
	if err != nil {
		return err
	}

	if _, ok := cf.Profiles[profile]; !ok {
		return fmt.Errorf("profile %q not found", profile)
	}

	delete(cf.Profiles, profile)

	// If the deleted profile was the default, clear default-profile.
	if cf.Root["default-profile"] == profile {
		delete(cf.Root, "default-profile")
	}

	return saveRaw(dataDir, cf)
}

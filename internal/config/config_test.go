package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()

	if err := Set(dir, "database-url", "postgres://test/db"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, err := Get(dir, "database-url")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "postgres://test/db" {
		t.Fatalf("got %q, want %q", val, "postgres://test/db")
	}
}

func TestOverwritePreservesOtherKeys(t *testing.T) {
	dir := t.TempDir()

	if err := Set(dir, "database-url", "first"); err != nil {
		t.Fatalf("Set database-url: %v", err)
	}
	if err := Set(dir, "server-port", "8080"); err != nil {
		t.Fatalf("Set server-port: %v", err)
	}
	if err := Set(dir, "database-url", "second"); err != nil {
		t.Fatalf("Set database-url again: %v", err)
	}

	val, err := Get(dir, "database-url")
	if err != nil {
		t.Fatalf("Get database-url: %v", err)
	}
	if val != "second" {
		t.Fatalf("database-url = %q, want %q", val, "second")
	}

	port, err := Get(dir, "server-port")
	if err != nil {
		t.Fatalf("Get server-port: %v", err)
	}
	if port != "8080" {
		t.Fatalf("server-port = %q, want %q", port, "8080")
	}
}

func TestFilePermissions(t *testing.T) {
	dir := t.TempDir()

	if err := Set(dir, "database-url", "postgres://test/db"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	info, err := os.Stat(Path(dir))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Fatalf("permissions = %o, want 0600", perm)
	}
}

func TestMissingDirectoryCreated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "deep")

	if err := Set(dir, "database-url", "postgres://test/db"); err != nil {
		t.Fatalf("Set with missing dir: %v", err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("expected directory to be created")
	}

	val, err := Get(dir, "database-url")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "postgres://test/db" {
		t.Fatalf("got %q, want %q", val, "postgres://test/db")
	}
}

func TestInvalidKeyRejected(t *testing.T) {
	dir := t.TempDir()

	err := Set(dir, "foo", "bar")
	if err == nil {
		t.Fatal("expected error for invalid key on Set")
	}
	if want := `unknown config key "foo"`; !contains(err.Error(), want) {
		t.Fatalf("Set error = %q, want containing %q", err, want)
	}
	if !contains(err.Error(), "database-url") {
		t.Fatalf("error should list valid keys, got: %q", err)
	}

	_, err = Get(dir, "invalid-key")
	if err == nil {
		t.Fatal("expected error for invalid key on Get")
	}
	if want := `unknown config key "invalid-key"`; !contains(err.Error(), want) {
		t.Fatalf("Get error = %q, want containing %q", err, want)
	}
}

func TestCorruptJSON(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(Path(dir), []byte("{corrupt!!!"), 0600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	_, err := Load(dir)
	if err == nil {
		t.Fatal("expected error for corrupt JSON")
	}
	if !contains(err.Error(), "corrupt") {
		t.Fatalf("error = %q, want containing 'corrupt'", err)
	}
}

func TestNoFileCreatedOnRead(t *testing.T) {
	dir := t.TempDir()

	val, err := Get(dir, "database-url")
	if err != nil {
		t.Fatalf("Get on missing file: %v", err)
	}
	if val != "" {
		t.Fatalf("expected empty value, got %q", val)
	}

	// Verify file was NOT created
	if _, statErr := os.Stat(Path(dir)); !os.IsNotExist(statErr) {
		t.Fatal("config file should not be created by Get")
	}
}

func TestAtomicWriteCleanup(t *testing.T) {
	dir := t.TempDir()

	if err := Set(dir, "server-port", "9090"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Verify no .tmp file lingers
	tmp := Path(dir) + ".tmp"
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("temp file should not exist after successful write")
	}
}

func TestConcurrentReads(t *testing.T) {
	dir := t.TempDir()

	// Seed config with all keys
	for _, key := range SortedKeys() {
		if err := Set(dir, key, "value-"+key); err != nil {
			t.Fatalf("seed Set(%s): %v", key, err)
		}
	}

	// Concurrent reads should never fail or return corrupt data
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg, err := Load(dir)
			if err != nil {
				t.Errorf("concurrent Load failed: %v", err)
				return
			}
			// Root keys only — profiles key is excluded from Load result.
			// Count root keys (excluding profiles-related).
			rootKeyCount := 0
			for range cfg {
				rootKeyCount++
			}
			if rootKeyCount != len(ValidKeys) {
				t.Errorf("expected %d keys, got %d", len(ValidKeys), rootKeyCount)
			}
		}()
	}
	wg.Wait()
}

func TestSequentialWriteIntegrity(t *testing.T) {
	dir := t.TempDir()

	// Sequential writes to different keys should all be preserved
	keys := SortedKeys()
	for i, key := range keys {
		value := "seq-value-" + key
		_ = i
		if err := Set(dir, key, value); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	// Verify all keys present via Load (root-level only)
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, key := range keys {
		want := "seq-value-" + key
		if got := cfg[key]; got != want {
			t.Fatalf("key %q = %q, want %q", key, got, want)
		}
	}
}

func TestPath(t *testing.T) {
	got := Path("/home/user/.engram")
	want := filepath.Join("/home/user/.engram", "config.json")
	if got != want {
		t.Fatalf("Path = %q, want %q", got, want)
	}
}

func TestSortedKeys(t *testing.T) {
	keys := SortedKeys()
	if len(keys) != len(ValidKeys) {
		t.Fatalf("SortedKeys length = %d, want %d", len(keys), len(ValidKeys))
	}
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Fatalf("keys not sorted: %q >= %q", keys[i-1], keys[i])
		}
	}
}

func TestValidKeyList(t *testing.T) {
	list := ValidKeyList()
	for key := range ValidKeys {
		if !contains(list, key) {
			t.Fatalf("ValidKeyList() missing %q: %s", key, list)
		}
	}
}

func TestLoadEmptyJSON(t *testing.T) {
	dir := t.TempDir()
	// Write "null" JSON — should return empty map, not nil
	if err := os.WriteFile(Path(dir), []byte("null"), 0600); err != nil {
		t.Fatalf("write null json: %v", err)
	}
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load null json: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil map for null JSON")
	}
	if len(cfg) != 0 {
		t.Fatalf("expected empty map, got %v", cfg)
	}
}

// ─── Profile tests ───────────────────────────────────────────────────────────

func TestDefaultProfileInValidKeys(t *testing.T) {
	if _, ok := ValidKeys["default-profile"]; !ok {
		t.Fatal("expected default-profile to be a valid key")
	}
}

func TestProfileSetAndGet(t *testing.T) {
	dir := t.TempDir()

	// Set root-level value
	if err := Set(dir, "database-url", "postgres://root/db"); err != nil {
		t.Fatalf("Set root: %v", err)
	}

	// Set profile-specific value
	if err := SetWithProfile(dir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile: %v", err)
	}

	// GetWithProfile should return profile value
	val, err := GetWithProfile(dir, "dev", "database-url")
	if err != nil {
		t.Fatalf("GetWithProfile: %v", err)
	}
	if val != "postgres://dev/db" {
		t.Fatalf("got %q, want %q", val, "postgres://dev/db")
	}

	// GetWithProfile with empty profile should fall back to root
	val, err = GetWithProfile(dir, "", "database-url")
	if err != nil {
		t.Fatalf("GetWithProfile empty profile: %v", err)
	}
	if val != "postgres://root/db" {
		t.Fatalf("got %q, want %q", val, "postgres://root/db")
	}

	// Plain Get should still return root
	val, err = Get(dir, "database-url")
	if err != nil {
		t.Fatalf("Get root: %v", err)
	}
	if val != "postgres://root/db" {
		t.Fatalf("got %q, want %q", val, "postgres://root/db")
	}
}

func TestProfileFallbackToRoot(t *testing.T) {
	dir := t.TempDir()

	// Set root-level server-port
	if err := Set(dir, "server-port", "9090"); err != nil {
		t.Fatalf("Set root: %v", err)
	}

	// Set profile with only database-url (no server-port)
	if err := SetWithProfile(dir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile: %v", err)
	}

	// GetWithProfile for server-port should fall back to root
	val, err := GetWithProfile(dir, "dev", "server-port")
	if err != nil {
		t.Fatalf("GetWithProfile: %v", err)
	}
	if val != "9090" {
		t.Fatalf("got %q, want %q", val, "9090")
	}
}

func TestProfileDefaultProfile(t *testing.T) {
	dir := t.TempDir()

	// Set default-profile
	if err := Set(dir, "default-profile", "prod"); err != nil {
		t.Fatalf("Set default-profile: %v", err)
	}

	// Set profile value
	if err := SetWithProfile(dir, "prod", "database-url", "postgres://prod/db"); err != nil {
		t.Fatalf("SetWithProfile: %v", err)
	}

	// GetWithProfile with empty profile should use default-profile
	val, err := GetWithProfile(dir, "", "database-url")
	if err != nil {
		t.Fatalf("GetWithProfile: %v", err)
	}
	if val != "postgres://prod/db" {
		t.Fatalf("got %q, want %q", val, "postgres://prod/db")
	}
}

func TestExplicitProfileOverridesDefault(t *testing.T) {
	dir := t.TempDir()

	// Set default-profile to prod
	if err := Set(dir, "default-profile", "prod"); err != nil {
		t.Fatalf("Set default-profile: %v", err)
	}

	// Set both profiles
	if err := SetWithProfile(dir, "prod", "database-url", "postgres://prod/db"); err != nil {
		t.Fatalf("SetWithProfile prod: %v", err)
	}
	if err := SetWithProfile(dir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile dev: %v", err)
	}

	// Explicit profile should override default-profile
	val, err := GetWithProfile(dir, "dev", "database-url")
	if err != nil {
		t.Fatalf("GetWithProfile: %v", err)
	}
	if val != "postgres://dev/db" {
		t.Fatalf("got %q, want %q", val, "postgres://dev/db")
	}
}

func TestListProfiles(t *testing.T) {
	dir := t.TempDir()

	// No profiles
	profiles, err := ListProfiles(dir)
	if err != nil {
		t.Fatalf("ListProfiles empty: %v", err)
	}
	if len(profiles) != 0 {
		t.Fatalf("expected empty profiles, got %v", profiles)
	}

	// Add profiles
	if err := SetWithProfile(dir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile dev: %v", err)
	}
	if err := SetWithProfile(dir, "prod", "database-url", "postgres://prod/db"); err != nil {
		t.Fatalf("SetWithProfile prod: %v", err)
	}
	if err := SetWithProfile(dir, "alpha", "database-url", "postgres://alpha/db"); err != nil {
		t.Fatalf("SetWithProfile alpha: %v", err)
	}

	profiles, err = ListProfiles(dir)
	if err != nil {
		t.Fatalf("ListProfiles: %v", err)
	}
	if len(profiles) != 3 {
		t.Fatalf("expected 3 profiles, got %d: %v", len(profiles), profiles)
	}
	// Should be sorted
	if profiles[0] != "alpha" || profiles[1] != "dev" || profiles[2] != "prod" {
		t.Fatalf("expected sorted [alpha dev prod], got %v", profiles)
	}
}

func TestSetWithProfileExcludedKeys(t *testing.T) {
	dir := t.TempDir()

	err := SetWithProfile(dir, "dev", "default-project", "my-project")
	if err == nil {
		t.Fatal("expected error for excluded key in profile")
	}
	if !contains(err.Error(), "cannot be set inside a profile") {
		t.Fatalf("error = %q, want containing 'cannot be set inside a profile'", err)
	}

	err = SetWithProfile(dir, "dev", "default-profile", "other")
	if err == nil {
		t.Fatal("expected error for excluded key in profile")
	}
}

func TestSetWithProfileEmptyName(t *testing.T) {
	dir := t.TempDir()

	err := SetWithProfile(dir, "", "database-url", "postgres://test/db")
	if err == nil {
		t.Fatal("expected error for empty profile name")
	}
	if !contains(err.Error(), "profile name cannot be empty") {
		t.Fatalf("error = %q, want containing 'profile name cannot be empty'", err)
	}
}

func TestSetWithProfileInvalidKey(t *testing.T) {
	dir := t.TempDir()

	err := SetWithProfile(dir, "dev", "invalid-key", "value")
	if err == nil {
		t.Fatal("expected error for invalid key in profile")
	}
	if !contains(err.Error(), "unknown config key") {
		t.Fatalf("error = %q, want containing 'unknown config key'", err)
	}
}

func TestGetWithProfileInvalidKey(t *testing.T) {
	dir := t.TempDir()

	_, err := GetWithProfile(dir, "dev", "invalid-key")
	if err == nil {
		t.Fatal("expected error for invalid key")
	}
}

func TestProfilePreservesRootOnSave(t *testing.T) {
	dir := t.TempDir()

	// Set root and profile values
	if err := Set(dir, "database-url", "postgres://root/db"); err != nil {
		t.Fatalf("Set root: %v", err)
	}
	if err := SetWithProfile(dir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile: %v", err)
	}

	// Update root value — profiles should be preserved
	if err := Set(dir, "server-port", "9090"); err != nil {
		t.Fatalf("Set server-port: %v", err)
	}

	// Verify profile still exists
	val, err := GetWithProfile(dir, "dev", "database-url")
	if err != nil {
		t.Fatalf("GetWithProfile after root update: %v", err)
	}
	if val != "postgres://dev/db" {
		t.Fatalf("profile value lost after root update: got %q", val)
	}
}

func TestGetProfileConfig(t *testing.T) {
	dir := t.TempDir()

	// No profile
	cfg, err := GetProfileConfig(dir, "nonexistent")
	if err != nil {
		t.Fatalf("GetProfileConfig: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil for nonexistent profile, got %v", cfg)
	}

	// Add profile
	if err := SetWithProfile(dir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile: %v", err)
	}
	if err := SetWithProfile(dir, "dev", "auth-method", "password"); err != nil {
		t.Fatalf("SetWithProfile auth-method: %v", err)
	}

	cfg, err = GetProfileConfig(dir, "dev")
	if err != nil {
		t.Fatalf("GetProfileConfig: %v", err)
	}
	if cfg["database-url"] != "postgres://dev/db" {
		t.Fatalf("database-url = %q, want %q", cfg["database-url"], "postgres://dev/db")
	}
	if cfg["auth-method"] != "password" {
		t.Fatalf("auth-method = %q, want %q", cfg["auth-method"], "password")
	}
}

func TestResolveProfile(t *testing.T) {
	t.Run("explicit overrides default", func(t *testing.T) {
		dir := t.TempDir()
		if err := Set(dir, "default-profile", "prod"); err != nil {
			t.Fatalf("Set: %v", err)
		}
		got := ResolveProfile(dir, "dev")
		if got != "dev" {
			t.Fatalf("ResolveProfile = %q, want %q", got, "dev")
		}
	})

	t.Run("uses default when no explicit", func(t *testing.T) {
		dir := t.TempDir()
		if err := Set(dir, "default-profile", "prod"); err != nil {
			t.Fatalf("Set: %v", err)
		}
		got := ResolveProfile(dir, "")
		if got != "prod" {
			t.Fatalf("ResolveProfile = %q, want %q", got, "prod")
		}
	})

	t.Run("returns empty when nothing set", func(t *testing.T) {
		dir := t.TempDir()
		got := ResolveProfile(dir, "")
		if got != "" {
			t.Fatalf("ResolveProfile = %q, want empty", got)
		}
	})
}

func TestProfileKeys(t *testing.T) {
	keys := ProfileKeys()
	for _, k := range keys {
		if profileExcludedKeys[k] {
			t.Fatalf("ProfileKeys includes excluded key %q", k)
		}
		if _, ok := ValidKeys[k]; !ok {
			t.Fatalf("ProfileKeys includes non-valid key %q", k)
		}
	}
	// Should not include excluded keys
	expected := len(ValidKeys) - len(profileExcludedKeys)
	if len(keys) != expected {
		t.Fatalf("ProfileKeys length = %d, want %d", len(keys), expected)
	}
}

func TestConfigFileStructureWithProfiles(t *testing.T) {
	dir := t.TempDir()

	// Set root values
	if err := Set(dir, "database-url", "postgres://root/db"); err != nil {
		t.Fatalf("Set root: %v", err)
	}
	if err := Set(dir, "default-profile", "dev"); err != nil {
		t.Fatalf("Set default-profile: %v", err)
	}

	// Set profile values
	if err := SetWithProfile(dir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile dev: %v", err)
	}
	if err := SetWithProfile(dir, "prod", "database-url", "postgres://prod/db"); err != nil {
		t.Fatalf("SetWithProfile prod: %v", err)
	}

	// Read the file directly and verify structure
	data, err := os.ReadFile(Path(dir))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// Verify root keys exist
	if _, ok := raw["database-url"]; !ok {
		t.Fatal("missing root key database-url")
	}
	if _, ok := raw["default-profile"]; !ok {
		t.Fatal("missing root key default-profile")
	}

	// Verify profiles section
	if _, ok := raw["profiles"]; !ok {
		t.Fatal("missing profiles section")
	}

	var profiles map[string]map[string]string
	if err := json.Unmarshal(raw["profiles"], &profiles); err != nil {
		t.Fatalf("Unmarshal profiles: %v", err)
	}
	if profiles["dev"]["database-url"] != "postgres://dev/db" {
		t.Fatalf("dev profile database-url = %q", profiles["dev"]["database-url"])
	}
	if profiles["prod"]["database-url"] != "postgres://prod/db" {
		t.Fatalf("prod profile database-url = %q", profiles["prod"]["database-url"])
	}
}

func TestBackwardCompatLoadWithProfiles(t *testing.T) {
	dir := t.TempDir()

	// Write a config file with profiles manually
	configJSON := `{
  "database-url": "postgres://root/db",
  "server-port": "7437",
  "profiles": {
    "dev": {
      "database-url": "postgres://dev/db"
    }
  }
}`
	if err := os.WriteFile(Path(dir), []byte(configJSON), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Load (backward compat) should return root-only
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg["database-url"] != "postgres://root/db" {
		t.Fatalf("database-url = %q, want root value", cfg["database-url"])
	}
	if cfg["server-port"] != "7437" {
		t.Fatalf("server-port = %q, want 7437", cfg["server-port"])
	}
	// Profiles should NOT appear in the flat map
	if _, ok := cfg["profiles"]; ok {
		t.Fatal("profiles key should not appear in Load result")
	}

	// Profile API should work
	val, err := GetWithProfile(dir, "dev", "database-url")
	if err != nil {
		t.Fatalf("GetWithProfile: %v", err)
	}
	if val != "postgres://dev/db" {
		t.Fatalf("got %q, want %q", val, "postgres://dev/db")
	}
}

func TestNoProfilesSectionWhenEmpty(t *testing.T) {
	dir := t.TempDir()

	if err := Set(dir, "database-url", "postgres://root/db"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	data, err := os.ReadFile(Path(dir))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if _, ok := raw["profiles"]; ok {
		t.Fatal("profiles section should not be present when no profiles exist")
	}
}

// contains checks if s contains substr (avoids importing strings in test).
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

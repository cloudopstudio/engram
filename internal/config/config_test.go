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
			if len(cfg) != len(ValidKeys) {
				t.Errorf("expected %d keys, got %d", len(ValidKeys), len(cfg))
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

	// Verify all keys present
	data, err := os.ReadFile(Path(dir))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var result map[string]string
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON after sequential writes: %v", err)
	}
	for _, key := range keys {
		want := "seq-value-" + key
		if got := result[key]; got != want {
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

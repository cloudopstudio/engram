package main

import (
	"context"
	"os"
	"testing"

	"github.com/Gentleman-Programming/engram/internal/store"
)

// TestTryStartAutosync_DisabledByDefault verifies that tryStartAutosync is a
// no-op when ENGRAM_CLOUD_AUTOSYNC is not set or not "1".
func TestTryStartAutosync_DisabledByDefault(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"unset", ""},
		{"zero", "0"},
		{"false", "false"},
		{"whitespace", "  "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value == "" {
				os.Unsetenv("ENGRAM_CLOUD_AUTOSYNC")
			} else {
				os.Setenv("ENGRAM_CLOUD_AUTOSYNC", tt.value)
				defer os.Unsetenv("ENGRAM_CLOUD_AUTOSYNC")
			}

			// Pass a nil store — should never be used when autosync is disabled.
			provider, stop := tryStartAutosync(context.Background(), nil, store.Config{})
			if provider != nil {
				t.Fatalf("expected nil status provider when autosync disabled, got %v", provider)
			}
			if stop != nil {
				t.Fatalf("expected nil stop func when autosync disabled, got non-nil")
			}
		})
	}
}

// TestTryStartAutosync_MissingToken verifies that tryStartAutosync is a no-op
// (with a log warning) when ENGRAM_CLOUD_AUTOSYNC=1 but token is not set.
func TestTryStartAutosync_MissingToken(t *testing.T) {
	os.Setenv("ENGRAM_CLOUD_AUTOSYNC", "1")
	os.Setenv("ENGRAM_CLOUD_SERVER", "https://cloud.example.com")
	os.Unsetenv("ENGRAM_CLOUD_TOKEN")
	defer func() {
		os.Unsetenv("ENGRAM_CLOUD_AUTOSYNC")
		os.Unsetenv("ENGRAM_CLOUD_SERVER")
	}()

	provider, stop := tryStartAutosync(context.Background(), nil, store.Config{DataDir: t.TempDir()})
	if provider != nil {
		t.Fatal("expected nil provider when token missing")
	}
	if stop != nil {
		t.Fatal("expected nil stop when token missing")
	}
}

// TestTryStartAutosync_MissingServer verifies that tryStartAutosync is a no-op
// when ENGRAM_CLOUD_AUTOSYNC=1 but server URL is not set.
func TestTryStartAutosync_MissingServer(t *testing.T) {
	os.Setenv("ENGRAM_CLOUD_AUTOSYNC", "1")
	os.Setenv("ENGRAM_CLOUD_TOKEN", "test-token")
	os.Unsetenv("ENGRAM_CLOUD_SERVER")
	defer func() {
		os.Unsetenv("ENGRAM_CLOUD_AUTOSYNC")
		os.Unsetenv("ENGRAM_CLOUD_TOKEN")
	}()

	provider, stop := tryStartAutosync(context.Background(), nil, store.Config{DataDir: t.TempDir()})
	if provider != nil {
		t.Fatal("expected nil provider when server URL missing")
	}
	if stop != nil {
		t.Fatal("expected nil stop when server URL missing")
	}
}

// TestTryStartAutosync_InvalidServerURL verifies that tryStartAutosync is a no-op
// when the server URL is not a valid http/https URL.
func TestTryStartAutosync_InvalidServerURL(t *testing.T) {
	os.Setenv("ENGRAM_CLOUD_AUTOSYNC", "1")
	os.Setenv("ENGRAM_CLOUD_TOKEN", "test-token")
	os.Setenv("ENGRAM_CLOUD_SERVER", "not-a-valid-url")
	defer func() {
		os.Unsetenv("ENGRAM_CLOUD_AUTOSYNC")
		os.Unsetenv("ENGRAM_CLOUD_TOKEN")
		os.Unsetenv("ENGRAM_CLOUD_SERVER")
	}()

	provider, stop := tryStartAutosync(context.Background(), nil, store.Config{DataDir: t.TempDir()})
	if provider != nil {
		t.Fatal("expected nil provider for invalid server URL")
	}
	if stop != nil {
		t.Fatal("expected nil stop for invalid server URL")
	}
}

// TestLoadCloudConfig_MissingFile verifies that loadCloudConfig returns nil, nil
// when cloud.json does not exist (not an error).
func TestLoadCloudConfig_MissingFile(t *testing.T) {
	cfg := store.Config{DataDir: t.TempDir()}
	cc, err := loadCloudConfig(cfg)
	if err != nil {
		t.Fatalf("loadCloudConfig: unexpected error for missing file: %v", err)
	}
	if cc != nil {
		t.Fatalf("loadCloudConfig: expected nil for missing file, got %+v", cc)
	}
}

// TestLoadCloudConfig_ValidFile verifies that loadCloudConfig reads cloud.json correctly.
func TestLoadCloudConfig_ValidFile(t *testing.T) {
	dir := t.TempDir()
	cfg := store.Config{DataDir: dir}

	if err := os.WriteFile(cloudConfigPath(cfg), []byte(`{"server_url":"https://cloud.example.com","token":"tok123"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	cc, err := loadCloudConfig(cfg)
	if err != nil {
		t.Fatalf("loadCloudConfig: %v", err)
	}
	if cc == nil {
		t.Fatal("expected non-nil cloudConfig")
	}
	if cc.ServerURL != "https://cloud.example.com" {
		t.Fatalf("ServerURL: got %q, want %q", cc.ServerURL, "https://cloud.example.com")
	}
	if cc.Token != "tok123" {
		t.Fatalf("Token: got %q, want %q", cc.Token, "tok123")
	}
}

// TestResolveCloudRuntimeConfig_EnvOverridesFile verifies that env vars override
// values from cloud.json and that persisted tokens are cleared.
func TestResolveCloudRuntimeConfig_EnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	cfg := store.Config{DataDir: dir}

	if err := os.WriteFile(cloudConfigPath(cfg), []byte(`{"server_url":"https://old.example.com","token":"old-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	os.Setenv("ENGRAM_CLOUD_SERVER", "https://new.example.com")
	os.Setenv("ENGRAM_CLOUD_TOKEN", "new-token")
	defer func() {
		os.Unsetenv("ENGRAM_CLOUD_SERVER")
		os.Unsetenv("ENGRAM_CLOUD_TOKEN")
	}()

	cc, err := resolveCloudRuntimeConfig(cfg)
	if err != nil {
		t.Fatalf("resolveCloudRuntimeConfig: %v", err)
	}
	if cc.ServerURL != "https://new.example.com" {
		t.Fatalf("ServerURL: got %q, want %q", cc.ServerURL, "https://new.example.com")
	}
	if cc.Token != "new-token" {
		t.Fatalf("Token: got %q, want %q", cc.Token, "new-token")
	}
}

// TestResolveCloudRuntimeConfig_PersistedTokenCleared verifies that a token
// stored in cloud.json is NOT returned (must come from env only).
func TestResolveCloudRuntimeConfig_PersistedTokenCleared(t *testing.T) {
	dir := t.TempDir()
	cfg := store.Config{DataDir: dir}

	if err := os.WriteFile(cloudConfigPath(cfg), []byte(`{"server_url":"https://cloud.example.com","token":"persisted-secret"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	os.Unsetenv("ENGRAM_CLOUD_TOKEN")
	os.Unsetenv("ENGRAM_CLOUD_SERVER")

	cc, err := resolveCloudRuntimeConfig(cfg)
	if err != nil {
		t.Fatalf("resolveCloudRuntimeConfig: %v", err)
	}
	if cc.Token != "" {
		t.Fatalf("expected Token to be cleared (must come from env), got %q", cc.Token)
	}
}

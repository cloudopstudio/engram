package main

import (
	"os"
	"strings"
	"testing"

	"github.com/Gentleman-Programming/engram/internal/config"
	versioncheck "github.com/Gentleman-Programming/engram/internal/version"
)

// ─── --auth-interactive flag parsing ─────────────────────────────────────────

func TestParseGlobalAuthInteractive(t *testing.T) {
	t.Run("bare flag returns true", func(t *testing.T) {
		withArgs(t, "engram", "--auth-interactive", "mcp")
		got := parseGlobalAuthInteractive()
		if !got {
			t.Fatal("expected true for bare --auth-interactive")
		}
		// os.Args should have --auth-interactive removed.
		if len(os.Args) != 2 || os.Args[1] != "mcp" {
			t.Fatalf("os.Args = %v, expected [engram mcp]", os.Args)
		}
	})

	t.Run("--auth-interactive=true returns true", func(t *testing.T) {
		withArgs(t, "engram", "--auth-interactive=true", "mcp")
		got := parseGlobalAuthInteractive()
		if !got {
			t.Fatal("expected true for --auth-interactive=true")
		}
		if len(os.Args) != 2 || os.Args[1] != "mcp" {
			t.Fatalf("os.Args = %v, expected [engram mcp]", os.Args)
		}
	})

	t.Run("--auth-interactive=false returns false", func(t *testing.T) {
		withArgs(t, "engram", "--auth-interactive=false", "mcp")
		got := parseGlobalAuthInteractive()
		if got {
			t.Fatal("expected false for --auth-interactive=false")
		}
		if len(os.Args) != 2 || os.Args[1] != "mcp" {
			t.Fatalf("os.Args = %v, expected [engram mcp]", os.Args)
		}
	})

	t.Run("no flag returns false", func(t *testing.T) {
		withArgs(t, "engram", "mcp")
		got := parseGlobalAuthInteractive()
		if got {
			t.Fatal("expected false when no --auth-interactive")
		}
		if len(os.Args) != 2 {
			t.Fatalf("os.Args should be unchanged: %v", os.Args)
		}
	})

	t.Run("flag with profile", func(t *testing.T) {
		withArgs(t, "engram", "--auth-interactive", "--profile", "devops", "mcp")
		got := parseGlobalAuthInteractive()
		if !got {
			t.Fatal("expected true for --auth-interactive with --profile")
		}
		// --auth-interactive removed, --profile and devops and mcp should remain.
		if len(os.Args) != 4 || os.Args[1] != "--profile" || os.Args[2] != "devops" || os.Args[3] != "mcp" {
			t.Fatalf("os.Args = %v, expected [engram --profile devops mcp]", os.Args)
		}
	})

	t.Run("--auth-interactive=1 returns true", func(t *testing.T) {
		withArgs(t, "engram", "--auth-interactive=1", "mcp")
		got := parseGlobalAuthInteractive()
		if !got {
			t.Fatal("expected true for --auth-interactive=1")
		}
	})
}

// ─── Config keys: tenant-id and client-id ────────────────────────────────────

func TestTenantIDAndClientIDAreValidConfigKeys(t *testing.T) {
	if _, ok := config.ValidKeys["tenant-id"]; !ok {
		t.Fatal("expected tenant-id to be a valid config key")
	}
	if _, ok := config.ValidKeys["client-id"]; !ok {
		t.Fatal("expected client-id to be a valid config key")
	}
}

func TestTenantIDAndClientIDConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()

	if err := config.Set(dir, "tenant-id", "my-tenant-123"); err != nil {
		t.Fatalf("Set tenant-id: %v", err)
	}
	if err := config.Set(dir, "client-id", "my-client-456"); err != nil {
		t.Fatalf("Set client-id: %v", err)
	}

	val, err := config.Get(dir, "tenant-id")
	if err != nil {
		t.Fatalf("Get tenant-id: %v", err)
	}
	if val != "my-tenant-123" {
		t.Fatalf("tenant-id = %q, want %q", val, "my-tenant-123")
	}

	val, err = config.Get(dir, "client-id")
	if err != nil {
		t.Fatalf("Get client-id: %v", err)
	}
	if val != "my-client-456" {
		t.Fatalf("client-id = %q, want %q", val, "my-client-456")
	}
}

func TestTenantIDAndClientIDProfileSupport(t *testing.T) {
	dir := t.TempDir()

	// Set root values
	if err := config.Set(dir, "tenant-id", "root-tenant"); err != nil {
		t.Fatalf("Set root tenant-id: %v", err)
	}

	// Set profile-specific values
	if err := config.SetWithProfile(dir, "devops", "tenant-id", "devops-tenant"); err != nil {
		t.Fatalf("SetWithProfile tenant-id: %v", err)
	}
	if err := config.SetWithProfile(dir, "devops", "client-id", "devops-client"); err != nil {
		t.Fatalf("SetWithProfile client-id: %v", err)
	}

	// Profile should override root
	val, err := config.GetWithProfile(dir, "devops", "tenant-id")
	if err != nil {
		t.Fatalf("GetWithProfile tenant-id: %v", err)
	}
	if val != "devops-tenant" {
		t.Fatalf("tenant-id = %q, want %q", val, "devops-tenant")
	}

	// No profile → fall back to root
	val, err = config.GetWithProfile(dir, "", "tenant-id")
	if err != nil {
		t.Fatalf("GetWithProfile empty profile tenant-id: %v", err)
	}
	if val != "root-tenant" {
		t.Fatalf("tenant-id = %q, want %q", val, "root-tenant")
	}

	// client-id not set at root → empty
	val, err = config.GetWithProfile(dir, "", "client-id")
	if err != nil {
		t.Fatalf("GetWithProfile empty profile client-id: %v", err)
	}
	if val != "" {
		t.Fatalf("client-id = %q, want empty", val)
	}
}

func TestTenantIDAndClientIDEnvVarMapping(t *testing.T) {
	info := config.ValidKeys["tenant-id"]
	if info.EnvVar != "AZURE_TENANT_ID" {
		t.Fatalf("tenant-id EnvVar = %q, want %q", info.EnvVar, "AZURE_TENANT_ID")
	}

	info = config.ValidKeys["client-id"]
	if info.EnvVar != "AZURE_CLIENT_ID" {
		t.Fatalf("client-id EnvVar = %q, want %q", info.EnvVar, "AZURE_CLIENT_ID")
	}
}

// ─── Login command exists in dispatch ────────────────────────────────────────

func TestMainDispatchLogin(t *testing.T) {
	stubExitWithPanic(t)
	stubCheckForUpdates(t, versioncheck.CheckResult{Status: versioncheck.StatusUpToDate})

	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)

	// Login should fail because it requires PostgreSQL backend, but it should
	// be recognized as a command (not "unknown command").
	withArgs(t, "engram", "login")
	_, stderr, recovered := captureOutputAndRecover(t, func() { main() })

	// With SQLite default backend, login must exit with the backend guard message.
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected exit for login without postgres backend, got %v", recovered)
	}
	if !strings.Contains(stderr, "requires PostgreSQL backend") {
		t.Fatalf("expected PostgreSQL backend message, got: %q", stderr)
	}
}

// ─── Usage text includes new flags and commands ──────────────────────────────

func TestPrintUsageIncludesAuthInteractiveAndLogin(t *testing.T) {
	stdout, _ := captureOutput(t, func() { printUsage() })

	if !strings.Contains(stdout, "--auth-interactive") {
		t.Fatalf("usage missing --auth-interactive flag: %q", stdout)
	}
	if !strings.Contains(stdout, "login") {
		t.Fatalf("usage missing login command: %q", stdout)
	}
	if !strings.Contains(stdout, "device code") {
		t.Fatalf("usage missing device code description: %q", stdout)
	}
}

// ─── AuthInteractive stored in Config ────────────────────────────────────────

func TestAuthInteractiveInMainFlow(t *testing.T) {
	stubExitWithPanic(t)
	stubCheckForUpdates(t, versioncheck.CheckResult{Status: versioncheck.StatusUpToDate})

	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)

	// With --auth-interactive, the login command should still be dispatched
	// (this tests that the flag is parsed AND the command still routes).
	withArgs(t, "engram", "--auth-interactive", "login")
	_, stderr, recovered := captureOutputAndRecover(t, func() { main() })

	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected exit, got %v", recovered)
	}
	// Should hit the backend guard (SQLite default → reject), proving the flag
	// was parsed and the command dispatched correctly.
	if !strings.Contains(stderr, "requires PostgreSQL backend") {
		t.Fatalf("expected PostgreSQL backend message, got: %q", stderr)
	}
}

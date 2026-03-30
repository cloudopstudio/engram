package main

import (
	"os"
	"strings"
	"testing"

	"github.com/Gentleman-Programming/engram/internal/config"
	versioncheck "github.com/Gentleman-Programming/engram/internal/version"
)

func TestCmdConfigPath(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	withArgs(t, "engram", "config", "path")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config path failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "config.json") {
		t.Fatalf("expected config.json in output, got: %q", stdout)
	}
}

func TestCmdConfigSetAndGet(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	// Set a value
	withArgs(t, "engram", "config", "set", "database-url", "postgres://localhost/engram")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config set failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "set database-url = postgres://localhost/engram") {
		t.Fatalf("unexpected set output: %q", stdout)
	}

	// Get the value back
	withArgs(t, "engram", "config", "get", "database-url")
	stdout, stderr, recovered = captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config get failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "postgres://localhost/engram") || !strings.Contains(stdout, "(config)") {
		t.Fatalf("unexpected get output: %q", stdout)
	}
}

func TestCmdConfigGetPriorityChain(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	// Set a config value
	if err := config.Set(cfg.DataDir, "server-port", "9090"); err != nil {
		t.Fatalf("config.Set: %v", err)
	}

	t.Run("env overrides config", func(t *testing.T) {
		t.Setenv("ENGRAM_PORT", "7000")
		withArgs(t, "engram", "config", "get", "server-port")
		stdout, _, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
		if recovered != nil {
			t.Fatalf("panic: %v", recovered)
		}
		if !strings.Contains(stdout, "7000") || !strings.Contains(stdout, "(env:") {
			t.Fatalf("expected env value, got: %q", stdout)
		}
	})

	t.Run("config value when no env", func(t *testing.T) {
		t.Setenv("ENGRAM_PORT", "")
		withArgs(t, "engram", "config", "get", "server-port")
		stdout, _, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
		if recovered != nil {
			t.Fatalf("panic: %v", recovered)
		}
		if !strings.Contains(stdout, "9090") || !strings.Contains(stdout, "(config)") {
			t.Fatalf("expected config value, got: %q", stdout)
		}
	})

	t.Run("default when nothing set", func(t *testing.T) {
		t.Setenv("ENGRAM_PORT", "")
		freshCfg := testConfig(t)
		withArgs(t, "engram", "config", "get", "server-port")
		stdout, _, recovered := captureOutputAndRecover(t, func() { cmdConfig(freshCfg) })
		if recovered != nil {
			t.Fatalf("panic: %v", recovered)
		}
		if !strings.Contains(stdout, "7437") || !strings.Contains(stdout, "(default)") {
			t.Fatalf("expected default value, got: %q", stdout)
		}
	})

	t.Run("not set for key with no default", func(t *testing.T) {
		t.Setenv("ENGRAM_DATABASE_URL", "")
		freshCfg := testConfig(t)
		withArgs(t, "engram", "config", "get", "database-url")
		stdout, _, recovered := captureOutputAndRecover(t, func() { cmdConfig(freshCfg) })
		if recovered != nil {
			t.Fatalf("panic: %v", recovered)
		}
		if !strings.Contains(stdout, "(not set)") {
			t.Fatalf("expected (not set), got: %q", stdout)
		}
	})
}

func TestCmdConfigList(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	if err := config.Set(cfg.DataDir, "database-url", "postgres://localhost/db"); err != nil {
		t.Fatalf("config.Set: %v", err)
	}

	t.Setenv("ENGRAM_PORT", "7000")
	t.Setenv("ENGRAM_DATABASE_URL", "")
	t.Setenv("ENGRAM_AUTH_METHOD", "")

	withArgs(t, "engram", "config", "list")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config list failed: panic=%v stderr=%q", recovered, stderr)
	}

	// database-url should show from config
	if !strings.Contains(stdout, "database-url") || !strings.Contains(stdout, "(config)") {
		t.Fatalf("expected database-url from config, got: %q", stdout)
	}
	// server-port should show from env
	if !strings.Contains(stdout, "server-port") || !strings.Contains(stdout, "(env)") {
		t.Fatalf("expected server-port from env, got: %q", stdout)
	}
}

func TestCmdConfigNoSubcommand(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	withArgs(t, "engram", "config")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected exit, got %v", recovered)
	}
	if !strings.Contains(stderr, "usage: engram config") {
		t.Fatalf("expected usage in stderr, got: %q", stderr)
	}
}

func TestCmdConfigUnknownSubcommand(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	withArgs(t, "engram", "config", "unknown")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected exit, got %v", recovered)
	}
	if !strings.Contains(stderr, "unknown config subcommand") {
		t.Fatalf("expected unknown subcommand error, got: %q", stderr)
	}
}

func TestCmdConfigSetMissingValue(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	withArgs(t, "engram", "config", "set", "database-url")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected exit, got %v", recovered)
	}
	if !strings.Contains(stderr, "usage: engram config set") {
		t.Fatalf("expected usage error, got: %q", stderr)
	}
}

func TestCmdConfigGetMissingKey(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	withArgs(t, "engram", "config", "get")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected exit, got %v", recovered)
	}
	if !strings.Contains(stderr, "usage: engram config get") {
		t.Fatalf("expected usage error, got: %q", stderr)
	}
}

func TestCmdConfigGetInvalidKey(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	withArgs(t, "engram", "config", "get", "invalid-key")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected exit, got %v", recovered)
	}
	if !strings.Contains(stderr, "unknown config key") {
		t.Fatalf("expected unknown key error, got: %q", stderr)
	}
}

func TestCmdConfigSetInvalidKey(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	withArgs(t, "engram", "config", "set", "bad-key", "value")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected exit, got %v", recovered)
	}
	if !strings.Contains(stderr, "unknown config key") {
		t.Fatalf("expected unknown key error, got: %q", stderr)
	}
}

func TestMainDispatchConfig(t *testing.T) {
	stubExitWithPanic(t)
	stubCheckForUpdates(t, versionCheckUpToDate())

	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)

	withArgs(t, "engram", "config", "path")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { main() })
	if recovered != nil || stderr != "" {
		t.Fatalf("main config dispatch failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "config.json") {
		t.Fatalf("expected config.json path, got: %q", stdout)
	}
}

// ─── Profile CLI tests ──────────────────────────────────────────────────────

func TestCmdConfigSetWithProfile(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	// Set a profile-specific value
	withArgs(t, "engram", "config", "set", "--profile", "dev", "database-url", "postgres://dev/db")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config set --profile failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "set database-url = postgres://dev/db") || !strings.Contains(stdout, "(profile: dev)") {
		t.Fatalf("unexpected set output: %q", stdout)
	}

	// Get back with profile
	withArgs(t, "engram", "config", "get", "--profile", "dev", "database-url")
	stdout, stderr, recovered = captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config get --profile failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "postgres://dev/db") || !strings.Contains(stdout, "(profile: dev)") {
		t.Fatalf("unexpected get output: %q", stdout)
	}
}

func TestCmdConfigGetProfileFallsBackToRoot(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	// Set root value
	if err := config.Set(cfg.DataDir, "server-port", "9090"); err != nil {
		t.Fatalf("config.Set: %v", err)
	}

	// Create profile without server-port
	if err := config.SetWithProfile(cfg.DataDir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("config.SetWithProfile: %v", err)
	}

	t.Setenv("ENGRAM_PORT", "")

	// Get server-port with --profile dev → should fall back to root
	withArgs(t, "engram", "config", "get", "--profile", "dev", "server-port")
	stdout, _, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil {
		t.Fatalf("panic: %v", recovered)
	}
	if !strings.Contains(stdout, "9090") || !strings.Contains(stdout, "(config)") {
		t.Fatalf("expected root fallback value, got: %q", stdout)
	}
}

func TestCmdConfigListWithProfile(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	// Set root and profile values
	if err := config.Set(cfg.DataDir, "database-url", "postgres://root/db"); err != nil {
		t.Fatalf("config.Set root: %v", err)
	}
	if err := config.SetWithProfile(cfg.DataDir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("config.SetWithProfile: %v", err)
	}
	if err := config.SetWithProfile(cfg.DataDir, "dev", "auth-method", "password"); err != nil {
		t.Fatalf("config.SetWithProfile auth-method: %v", err)
	}

	t.Setenv("ENGRAM_DATABASE_URL", "")
	t.Setenv("ENGRAM_AUTH_METHOD", "")
	t.Setenv("ENGRAM_PORT", "")

	withArgs(t, "engram", "config", "list", "--profile", "dev")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config list --profile failed: panic=%v stderr=%q", recovered, stderr)
	}

	// Should show profile header
	if !strings.Contains(stdout, "Profile: dev") {
		t.Fatalf("expected profile header, got: %q", stdout)
	}
	// database-url should show from profile
	if !strings.Contains(stdout, "postgres://dev/db") || !strings.Contains(stdout, "(profile: dev)") {
		t.Fatalf("expected profile database-url, got: %q", stdout)
	}
	// auth-method should show from profile
	if !strings.Contains(stdout, "password") || !strings.Contains(stdout, "(profile: dev)") {
		t.Fatalf("expected profile auth-method, got: %q", stdout)
	}
}

func TestCmdConfigListNonexistentProfile(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	withArgs(t, "engram", "config", "list", "--profile", "nonexistent")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected exit, got %v", recovered)
	}
	if !strings.Contains(stderr, "not found") {
		t.Fatalf("expected not found error, got: %q", stderr)
	}
}

func TestCmdConfigProfilesSubcommand(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	// No profiles
	withArgs(t, "engram", "config", "profiles")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config profiles empty failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "No profiles configured") {
		t.Fatalf("expected empty profiles message, got: %q", stdout)
	}

	// Add profiles
	if err := config.SetWithProfile(cfg.DataDir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile dev: %v", err)
	}
	if err := config.SetWithProfile(cfg.DataDir, "prod", "database-url", "postgres://prod/db"); err != nil {
		t.Fatalf("SetWithProfile prod: %v", err)
	}
	if err := config.Set(cfg.DataDir, "default-profile", "prod"); err != nil {
		t.Fatalf("Set default-profile: %v", err)
	}

	withArgs(t, "engram", "config", "profiles")
	stdout, stderr, recovered = captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config profiles failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "dev") {
		t.Fatalf("expected dev profile, got: %q", stdout)
	}
	if !strings.Contains(stdout, "prod") || !strings.Contains(stdout, "(default)") {
		t.Fatalf("expected prod profile with default marker, got: %q", stdout)
	}
}

func TestCmdConfigGetUsesDefaultProfile(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	// Set default-profile
	if err := config.Set(cfg.DataDir, "default-profile", "staging"); err != nil {
		t.Fatalf("Set default-profile: %v", err)
	}
	// Set profile value
	if err := config.SetWithProfile(cfg.DataDir, "staging", "database-url", "postgres://staging/db"); err != nil {
		t.Fatalf("SetWithProfile: %v", err)
	}

	t.Setenv("ENGRAM_DATABASE_URL", "")

	// Get without --profile should use default-profile
	withArgs(t, "engram", "config", "get", "database-url")
	stdout, _, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil {
		t.Fatalf("panic: %v", recovered)
	}
	if !strings.Contains(stdout, "postgres://staging/db") || !strings.Contains(stdout, "(profile: staging)") {
		t.Fatalf("expected default profile value, got: %q", stdout)
	}
}

func TestCmdConfigSetDefaultProfile(t *testing.T) {
	cfg := testConfig(t)
	stubExitWithPanic(t)

	// Set default-profile as a root config value
	withArgs(t, "engram", "config", "set", "default-profile", "arquitectura")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("config set default-profile failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "set default-profile = arquitectura") {
		t.Fatalf("unexpected set output: %q", stdout)
	}

	// Verify it's saved
	val, err := config.Get(cfg.DataDir, "default-profile")
	if err != nil {
		t.Fatalf("config.Get: %v", err)
	}
	if val != "arquitectura" {
		t.Fatalf("default-profile = %q, want %q", val, "arquitectura")
	}
}

func TestParseGlobalProfile(t *testing.T) {
	t.Run("extracts profile and removes from args", func(t *testing.T) {
		dataDir := t.TempDir()
		withArgs(t, "engram", "--profile", "dev", "mcp")
		profile := parseGlobalProfile(dataDir)
		if profile != "dev" {
			t.Fatalf("profile = %q, want %q", profile, "dev")
		}
		// os.Args should have --profile and dev removed
		if len(os.Args) != 2 || os.Args[1] != "mcp" {
			t.Fatalf("os.Args = %v, expected [engram mcp]", os.Args)
		}
	})

	t.Run("extracts profile with equals sign", func(t *testing.T) {
		dataDir := t.TempDir()
		withArgs(t, "engram", "--profile=arquitectura", "mcp")
		profile := parseGlobalProfile(dataDir)
		if profile != "arquitectura" {
			t.Fatalf("profile = %q, want %q", profile, "arquitectura")
		}
		// os.Args should have --profile=arquitectura removed
		if len(os.Args) != 2 || os.Args[1] != "mcp" {
			t.Fatalf("os.Args = %v, expected [engram mcp]", os.Args)
		}
	})

	t.Run("uses default-profile when no flag", func(t *testing.T) {
		dataDir := t.TempDir()
		if err := config.Set(dataDir, "default-profile", "prod"); err != nil {
			t.Fatalf("Set: %v", err)
		}
		withArgs(t, "engram", "mcp")
		profile := parseGlobalProfile(dataDir)
		if profile != "prod" {
			t.Fatalf("profile = %q, want %q", profile, "prod")
		}
	})

	t.Run("returns empty when nothing set", func(t *testing.T) {
		dataDir := t.TempDir()
		withArgs(t, "engram", "mcp")
		profile := parseGlobalProfile(dataDir)
		if profile != "" {
			t.Fatalf("profile = %q, want empty", profile)
		}
	})
}

// ─── delete-profile CLI tests ────────────────────────────────────────────────

func TestCmdConfigDeleteProfile(t *testing.T) {
	t.Run("deletes existing profile", func(t *testing.T) {
		cfg := testConfig(t)
		stubExitWithPanic(t)

		// Create a profile
		if err := config.SetWithProfile(cfg.DataDir, "dev", "database-url", "postgres://dev/db"); err != nil {
			t.Fatalf("SetWithProfile: %v", err)
		}

		withArgs(t, "engram", "config", "delete-profile", "dev")
		stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
		if recovered != nil || stderr != "" {
			t.Fatalf("delete-profile failed: panic=%v stderr=%q", recovered, stderr)
		}
		if !strings.Contains(stdout, `deleted profile "dev"`) {
			t.Fatalf("unexpected output: %q", stdout)
		}

		// Verify profile is gone
		profiles, err := config.ListProfiles(cfg.DataDir)
		if err != nil {
			t.Fatalf("ListProfiles: %v", err)
		}
		if len(profiles) != 0 {
			t.Fatalf("expected no profiles, got %v", profiles)
		}
	})

	t.Run("errors on nonexistent profile", func(t *testing.T) {
		cfg := testConfig(t)
		stubExitWithPanic(t)

		withArgs(t, "engram", "config", "delete-profile", "ghost")
		_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
		if _, ok := recovered.(exitCode); !ok {
			t.Fatalf("expected exit, got %v", recovered)
		}
		if !strings.Contains(stderr, "not found") {
			t.Fatalf("expected not found error, got: %q", stderr)
		}
	})

	t.Run("errors when no name given", func(t *testing.T) {
		cfg := testConfig(t)
		stubExitWithPanic(t)

		withArgs(t, "engram", "config", "delete-profile")
		_, stderr, recovered := captureOutputAndRecover(t, func() { cmdConfig(cfg) })
		if _, ok := recovered.(exitCode); !ok {
			t.Fatalf("expected exit, got %v", recovered)
		}
		if !strings.Contains(stderr, "usage: engram config delete-profile") {
			t.Fatalf("expected usage error, got: %q", stderr)
		}
	})
}

// ─── Non-existent profile warning test ───────────────────────────────────────

func TestMainWarnsOnNonexistentProfile(t *testing.T) {
	stubExitWithPanic(t)
	stubCheckForUpdates(t, versionCheckUpToDate())

	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)

	// Use a profile that doesn't exist → should warn on stderr
	withArgs(t, "engram", "--profile", "ghost", "version")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { main() })
	if recovered != nil {
		t.Fatalf("unexpected panic: %v", recovered)
	}
	if !strings.Contains(stderr, `warning: profile "ghost" not found`) {
		t.Fatalf("expected warning on stderr, got: %q", stderr)
	}
	// stdout should still work (version command)
	if !strings.Contains(stdout, "engram") {
		t.Fatalf("expected version output on stdout, got: %q", stdout)
	}
}

func TestMainNoWarningForExistingProfile(t *testing.T) {
	stubExitWithPanic(t)
	stubCheckForUpdates(t, versionCheckUpToDate())

	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)

	// Create the profile
	if err := config.SetWithProfile(dataDir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile: %v", err)
	}

	withArgs(t, "engram", "--profile", "dev", "version")
	_, stderr, recovered := captureOutputAndRecover(t, func() { main() })
	if recovered != nil {
		t.Fatalf("unexpected panic: %v", recovered)
	}
	if strings.Contains(stderr, "warning") {
		t.Fatalf("expected no warning for existing profile, got: %q", stderr)
	}
}

func TestMainDispatchWithEqualsProfile(t *testing.T) {
	stubExitWithPanic(t)
	stubCheckForUpdates(t, versionCheckUpToDate())

	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)

	// Set up a profile
	if err := config.SetWithProfile(dataDir, "arquitectura", "database-url", "postgres://arq/db"); err != nil {
		t.Fatalf("SetWithProfile: %v", err)
	}

	t.Setenv("ENGRAM_DATABASE_URL", "")

	// Use --profile=arquitectura syntax
	withArgs(t, "engram", "--profile=arquitectura", "config", "get", "database-url")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { main() })
	if recovered != nil || stderr != "" {
		t.Fatalf("main --profile= dispatch failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "postgres://arq/db") {
		t.Fatalf("expected profile value, got: %q", stdout)
	}
}

func TestMainDispatchWithProfile(t *testing.T) {
	stubExitWithPanic(t)
	stubCheckForUpdates(t, versionCheckUpToDate())

	dataDir := t.TempDir()
	t.Setenv("ENGRAM_DATA_DIR", dataDir)

	// Set up a profile
	if err := config.SetWithProfile(dataDir, "dev", "database-url", "postgres://dev/db"); err != nil {
		t.Fatalf("SetWithProfile: %v", err)
	}

	// Run: engram --profile dev config get database-url
	// The --profile flag should be parsed and removed before command dispatch
	t.Setenv("ENGRAM_DATABASE_URL", "")

	withArgs(t, "engram", "--profile", "dev", "config", "get", "database-url")
	stdout, stderr, recovered := captureOutputAndRecover(t, func() { main() })
	if recovered != nil || stderr != "" {
		t.Fatalf("main --profile dispatch failed: panic=%v stderr=%q", recovered, stderr)
	}
	if !strings.Contains(stdout, "postgres://dev/db") {
		t.Fatalf("expected profile value, got: %q", stdout)
	}
}

func versionCheckUpToDate() versioncheck.CheckResult {
	return versioncheck.CheckResult{Status: versioncheck.StatusUpToDate}
}

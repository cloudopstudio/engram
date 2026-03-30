package main

import (
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

func versionCheckUpToDate() versioncheck.CheckResult {
	return versioncheck.CheckResult{Status: versioncheck.StatusUpToDate}
}

package main

import (
	"net/http/httptest"
	"net/http"
	"os"
	"testing"

	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
)

// ─── normalizeTransportType ───────────────────────────────────────────────────

func TestNormalizeTransportTypeFile(t *testing.T) {
	for _, raw := range []string{"file", "FILE", "File", ""} {
		got, err := normalizeTransportType(raw)
		if err != nil {
			t.Fatalf("normalizeTransportType(%q): unexpected error: %v", raw, err)
		}
		if got != "file" {
			t.Fatalf("normalizeTransportType(%q) = %q, want %q", raw, got, "file")
		}
	}
}

func TestNormalizeTransportTypeHTTP(t *testing.T) {
	for _, raw := range []string{"http", "HTTP", "https", "HTTPS"} {
		got, err := normalizeTransportType(raw)
		if err != nil {
			t.Fatalf("normalizeTransportType(%q): unexpected error: %v", raw, err)
		}
		if got != "http" {
			t.Fatalf("normalizeTransportType(%q) = %q, want %q", raw, got, "http")
		}
	}
}

func TestNormalizeTransportTypeUnknown(t *testing.T) {
	_, err := normalizeTransportType("grpc")
	if err == nil {
		t.Fatal("expected error for unknown transport type")
	}
}

// ─── defaultNewSyncTransport ──────────────────────────────────────────────────

func TestDefaultNewSyncTransportFileReturnsFileTransport(t *testing.T) {
	dir := t.TempDir()
	tr, err := defaultNewSyncTransport("file", dir, "proj")
	if err != nil {
		t.Fatalf("defaultNewSyncTransport file: %v", err)
	}
	if _, ok := tr.(*engramsync.FileTransport); !ok {
		t.Fatalf("expected *engramsync.FileTransport, got %T", tr)
	}
}

func TestDefaultNewSyncTransportHTTPRequiresEngramRemoteURL(t *testing.T) {
	t.Setenv("ENGRAM_REMOTE_URL", "")
	_, err := defaultNewSyncTransport("http", ".engram", "proj")
	if err == nil {
		t.Fatal("expected error when ENGRAM_REMOTE_URL is empty")
	}
}

func TestDefaultNewSyncTransportHTTPReturnsHTTPTransport(t *testing.T) {
	// Use a real httptest server so the URL is valid.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("ENGRAM_REMOTE_URL", srv.URL)
	t.Setenv("ENGRAM_REMOTE_TOKEN", "test-token")

	tr, err := defaultNewSyncTransport("http", ".engram", "proj-test")
	if err != nil {
		t.Fatalf("defaultNewSyncTransport http: %v", err)
	}
	if tr == nil {
		t.Fatal("expected non-nil transport")
	}
	// Verify it satisfies the Transport interface (compile-time check).
	var _ engramsync.Transport = tr
}

// ─── ENGRAM_TRANSPORT env resolution ─────────────────────────────────────────

func TestCmdSyncTransportFlagResolvesToHTTPViaEnv(t *testing.T) {
	// We only test the transport resolution logic (newSyncTransport injection),
	// not the full cmdSync flow (which would require a real store + server).
	resolved := ""
	old := newSyncTransport
	newSyncTransport = func(transportType, syncDir, project string) (engramsync.Transport, error) {
		resolved = transportType
		return engramsync.NewFileTransport(syncDir), nil // return file to avoid network calls
	}
	t.Cleanup(func() { newSyncTransport = old })

	t.Setenv("ENGRAM_TRANSPORT", "http")

	// Call normalizeTransportType to confirm env-resolved value is "http".
	raw := os.Getenv("ENGRAM_TRANSPORT")
	got, err := normalizeTransportType(raw)
	if err != nil {
		t.Fatalf("normalizeTransportType: %v", err)
	}
	if got != "http" {
		t.Fatalf("expected http, got %q", got)
	}

	// Simulate what cmdSync does: call newSyncTransport with the resolved type.
	if _, err := newSyncTransport(got, ".engram", "proj"); err != nil {
		t.Fatalf("newSyncTransport: %v", err)
	}
	if resolved != "http" {
		t.Fatalf("expected resolved=http, got %q", resolved)
	}
}

func TestCmdSyncTransportDefaultIsFile(t *testing.T) {
	// Without any flag or env, transport should resolve to "file".
	t.Setenv("ENGRAM_TRANSPORT", "")
	got, err := normalizeTransportType("")
	if err != nil {
		t.Fatalf("normalizeTransportType: %v", err)
	}
	if got != "file" {
		t.Fatalf("expected file, got %q", got)
	}
}

func TestCmdSyncTransportInvalidValueReturnsError(t *testing.T) {
	_, err := normalizeTransportType("websocket")
	if err == nil {
		t.Fatal("expected error for invalid transport value")
	}
}

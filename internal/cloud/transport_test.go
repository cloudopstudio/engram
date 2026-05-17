package cloud

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Gentleman-Programming/engram/internal/cloud/chunkcodec"
	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
)

// ─── NewHTTPTransport validation ─────────────────────────────────────────────

func TestNewHTTPTransportRejectsEmptyURL(t *testing.T) {
	_, err := NewHTTPTransport("", "tok", "proj")
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestNewHTTPTransportRejectsNonHTTPScheme(t *testing.T) {
	_, err := NewHTTPTransport("ftp://example.com", "tok", "proj")
	if err == nil {
		t.Fatal("expected error for ftp:// scheme")
	}
}

func TestNewHTTPTransportRejectsURLWithQuery(t *testing.T) {
	_, err := NewHTTPTransport("http://example.com?foo=bar", "tok", "proj")
	if err == nil {
		t.Fatal("expected error for URL with query string")
	}
}

func TestNewHTTPTransportRejectsEmptyProject(t *testing.T) {
	_, err := NewHTTPTransport("http://example.com", "tok", "")
	if err == nil {
		t.Fatal("expected error for empty project")
	}
}

func TestNewHTTPTransportOK(t *testing.T) {
	ht, err := NewHTTPTransport("http://example.com/", "tok", "my-project")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ht == nil {
		t.Fatal("expected non-nil transport")
	}
}

// ─── ReadManifest ─────────────────────────────────────────────────────────────

func TestReadManifestHappyPath(t *testing.T) {
	manifest := engramsync.Manifest{
		Version: 1,
		Chunks: []engramsync.ChunkEntry{
			{ID: "abc12345", CreatedBy: "alice", CreatedAt: "2026-01-01T00:00:00Z"},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/sync/pull" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("project") != "proj-a" {
			t.Errorf("expected project=proj-a, got %q", r.URL.Query().Get("project"))
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(manifest); err != nil {
			t.Errorf("encode manifest: %v", err)
		}
	}))
	defer srv.Close()

	ht, err := NewHTTPTransport(srv.URL, "token", "proj-a")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}

	got, err := ht.ReadManifest()
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.Version != 1 || len(got.Chunks) != 1 || got.Chunks[0].ID != "abc12345" {
		t.Fatalf("unexpected manifest: %+v", got)
	}
}

func TestReadManifestHardFailOn500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	ht, err := NewHTTPTransport(srv.URL, "tok", "proj")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}

	_, err = ht.ReadManifest()
	if err == nil {
		t.Fatal("expected error on 500 response")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected *HTTPStatusError, got %T: %v", err, err)
	}
	if statusErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", statusErr.StatusCode)
	}
}

func TestReadManifestHardFailOn401(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	ht, err := NewHTTPTransport(srv.URL, "bad-token", "proj")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}

	_, err = ht.ReadManifest()
	if err == nil {
		t.Fatal("expected error on 401 response")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected *HTTPStatusError, got %T", err)
	}
	if !statusErr.IsAuthFailure() {
		t.Fatal("expected IsAuthFailure=true")
	}
}

func TestReadManifestParsesRepairableErrorClass(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error_class":"repairable","error_code":"upgrade_repairable_payload_invalid","error":"sessions[0].directory is required"}`))
	}))
	defer srv.Close()

	ht, err := NewHTTPTransport(srv.URL, "tok", "proj")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}

	_, err = ht.ReadManifest()
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected *HTTPStatusError, got %T", err)
	}
	if !statusErr.IsRepairableMigrationFailure() {
		t.Fatalf("expected IsRepairableMigrationFailure=true, got false")
	}
	if statusErr.ErrorCode != "upgrade_repairable_payload_invalid" {
		t.Fatalf("expected error_code 'upgrade_repairable_payload_invalid', got %q", statusErr.ErrorCode)
	}
}

// ─── WriteManifest ─────────────────────────────────────────────────────────────

func TestWriteManifestIsNoOp(t *testing.T) {
	ht, err := NewHTTPTransport("http://example.com", "tok", "proj")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}
	// Must not make any HTTP calls; no server is needed.
	if err := ht.WriteManifest(&engramsync.Manifest{Version: 1}); err != nil {
		t.Fatalf("WriteManifest should be a no-op, got: %v", err)
	}
}

// ─── WriteChunk ───────────────────────────────────────────────────────────────

func TestWriteChunkHappyPath(t *testing.T) {
	var gotChunkID string
	var gotData json.RawMessage
	var gotCreatedAt string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/sync/push" {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		var req struct {
			ChunkID         string          `json:"chunk_id"`
			ClientCreatedAt string          `json:"client_created_at"`
			Data            json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		gotChunkID = req.ChunkID
		gotCreatedAt = req.ClientCreatedAt
		gotData = req.Data
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	ht, err := NewHTTPTransport(srv.URL, "tok", "proj-a")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}

	originalPayload := []byte(`{"sessions":[{"id":"s-1","directory":"/tmp/s-1"}],"observations":[],"prompts":[]}`)
	entry := engramsync.ChunkEntry{CreatedBy: "tester", CreatedAt: "2026-01-01T00:00:00Z"}

	if err := ht.WriteChunk("deadbeef", originalPayload, entry); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	canonicalPayload, _ := chunkcodec.CanonicalizeForProject(originalPayload, "proj-a")
	wantChunkID := chunkcodec.ChunkID(canonicalPayload)

	if gotChunkID != wantChunkID {
		t.Fatalf("expected chunk_id %q, got %q", wantChunkID, gotChunkID)
	}
	if gotCreatedAt != "2026-01-01T00:00:00Z" {
		t.Fatalf("expected client_created_at %q, got %q", "2026-01-01T00:00:00Z", gotCreatedAt)
	}
	if strings.TrimSpace(string(gotData)) != strings.TrimSpace(string(canonicalPayload)) {
		t.Fatalf("data mismatch: got %s want %s", gotData, canonicalPayload)
	}
}

func TestWriteChunkHardFailOn500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	ht, err := NewHTTPTransport(srv.URL, "tok", "proj")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}

	payload := []byte(`{"sessions":[],"observations":[],"prompts":[]}`)
	err = ht.WriteChunk("aabb1234", payload, engramsync.ChunkEntry{})
	if err == nil {
		t.Fatal("expected error on 500 response")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected *HTTPStatusError, got %T: %v", err, err)
	}
	if statusErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", statusErr.StatusCode)
	}
}

// ─── ReadChunk ────────────────────────────────────────────────────────────────

func TestReadChunkHappyPath(t *testing.T) {
	chunkData := []byte(`{"sessions":[{"id":"s-1","project":"proj"}],"observations":[],"prompts":[]}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Path: /sync/pull/abc12345
		if !strings.HasSuffix(r.URL.Path, "/abc12345") {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("project") != "proj" {
			t.Errorf("expected project=proj, got %q", r.URL.Query().Get("project"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(chunkData)
	}))
	defer srv.Close()

	ht, err := NewHTTPTransport(srv.URL, "tok", "proj")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}

	got, err := ht.ReadChunk("abc12345")
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if strings.TrimSpace(string(got)) != strings.TrimSpace(string(chunkData)) {
		t.Fatalf("data mismatch: got %s want %s", got, chunkData)
	}
}

func TestReadChunkReturnsErrChunkNotFoundOn404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	ht, err := NewHTTPTransport(srv.URL, "tok", "proj")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}

	_, err = ht.ReadChunk("nonexistent")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !errors.Is(err, engramsync.ErrChunkNotFound) {
		t.Fatalf("expected ErrChunkNotFound, got %v", err)
	}
}

func TestReadChunkHardFailOn500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	ht, err := NewHTTPTransport(srv.URL, "tok", "proj")
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}

	_, err = ht.ReadChunk("somechunk")
	if err == nil {
		t.Fatal("expected error on 500 response")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected *HTTPStatusError, got %T: %v", err, err)
	}
	if statusErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", statusErr.StatusCode)
	}
}

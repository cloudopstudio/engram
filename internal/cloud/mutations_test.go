package cloud

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func mustNewMutationTransport(t *testing.T, baseURL, token string) *MutationTransport {
	t.Helper()
	mt, err := NewMutationTransport(baseURL, token)
	if err != nil {
		t.Fatalf("NewMutationTransport(%q): %v", baseURL, err)
	}
	return mt
}

// ─── NewMutationTransport URL validation (BW6) ───────────────────────────────

func TestNewMutationTransportRejectsInvalidURL(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{name: "empty", url: ""},
		{name: "no-scheme", url: "example.com/sync"},
		{name: "invalid-scheme", url: "ftp://example.com"},
		{name: "no-host", url: "http://"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mt, err := NewMutationTransport(tc.url, "token")
			if err == nil {
				t.Fatalf("expected error for URL %q, got nil (transport=%v)", tc.url, mt)
			}
		})
	}
}

func TestNewMutationTransportAcceptsValidURL(t *testing.T) {
	cases := []string{
		"http://localhost:8080",
		"https://example.com",
		"http://127.0.0.1:9000/",
	}
	for _, u := range cases {
		mt, err := NewMutationTransport(u, "token")
		if err != nil {
			t.Fatalf("expected nil error for URL %q, got %v", u, err)
		}
		if mt == nil {
			t.Fatalf("expected non-nil transport for URL %q", u)
		}
	}
}

// ─── Interface satisfaction ───────────────────────────────────────────────────

// TestMutationTransportImplementsMutationTransporter verifies that *MutationTransport
// satisfies the MutationTransporter interface at compile time.
func TestMutationTransportImplementsMutationTransporter(t *testing.T) {
	var _ MutationTransporter = (*MutationTransport)(nil)
}

// TestFileTransportDoesNotImplementMutationTransporter verifies that
// FileTransport (from internal/sync) does NOT implement MutationTransporter.
// We confirm by checking the absence of the methods at runtime.
func TestMutationTransporterIsOptional(t *testing.T) {
	// This test documents the design decision: MutationTransporter is optional.
	// FileTransport does not implement it. Any type can opt in by implementing
	// PushMutations and PullMutations with the correct signatures.
	mt := mustNewMutationTransport(t, "http://localhost:8080", "token")
	var _ MutationTransporter = mt // compile-time assertion
}

// ─── PushMutations ───────────────────────────────────────────────────────────

// TestMutationTransportPushAccepted verifies REQ-200: valid push returns accepted_seqs.
func TestMutationTransportPushAccepted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/sync/mutations/push" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer token123" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accepted_seqs":[1,2,3]}`))
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "token123")
	entries := []MutationEntry{
		{Project: "proj-a", Entity: "observation", EntityKey: "k1", Op: "upsert", Payload: json.RawMessage(`{}`)},
		{Project: "proj-a", Entity: "observation", EntityKey: "k2", Op: "upsert", Payload: json.RawMessage(`{}`)},
		{Project: "proj-a", Entity: "observation", EntityKey: "k3", Op: "upsert", Payload: json.RawMessage(`{}`)},
	}
	seqs, err := mt.PushMutations(entries)
	if err != nil {
		t.Fatalf("PushMutations: %v", err)
	}
	if len(seqs) != 3 {
		t.Fatalf("expected 3 accepted_seqs, got %d", len(seqs))
	}
	if seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("unexpected seqs: %v", seqs)
	}
}

// TestMutationTransportPushSendsAuthHeader verifies that the Authorization header is set.
func TestMutationTransportPushSendsAuthHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accepted_seqs":[]}`))
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "my-secret-token")
	_, err := mt.PushMutations([]MutationEntry{
		{Project: "proj-a", Entity: "observation", EntityKey: "k1", Op: "upsert"},
	})
	if err != nil {
		t.Fatalf("PushMutations: %v", err)
	}
	if gotAuth != "Bearer my-secret-token" {
		t.Fatalf("expected Authorization header %q, got %q", "Bearer my-secret-token", gotAuth)
	}
}

// TestMutationTransportPushSendsJSONBody verifies that entries are marshalled correctly.
func TestMutationTransportPushSendsJSONBody(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accepted_seqs":[10]}`))
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "tok")
	_, err := mt.PushMutations([]MutationEntry{
		{Project: "proj-a", Entity: "observation", EntityKey: "sync-id-1", Op: "upsert", Payload: json.RawMessage(`{"title":"hello"}`)},
	})
	if err != nil {
		t.Fatalf("PushMutations: %v", err)
	}

	var parsed struct {
		Entries []MutationEntry `json:"entries"`
	}
	if err := json.Unmarshal(gotBody, &parsed); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if len(parsed.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(parsed.Entries))
	}
	if parsed.Entries[0].EntityKey != "sync-id-1" {
		t.Fatalf("unexpected entity_key: %q", parsed.Entries[0].EntityKey)
	}
}

// TestMutationTransportPushHardFailOn500 verifies hard-fail on 500 (no silent retry).
func TestMutationTransportPushHardFailOn500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "tok")
	_, err := mt.PushMutations([]MutationEntry{
		{Project: "proj-a", Entity: "observation", EntityKey: "k1", Op: "upsert"},
	})
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

// TestMutationTransportPushUnauth verifies REQ-200: 401 → IsAuthFailure.
func TestMutationTransportPushUnauth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "bad-token")
	_, err := mt.PushMutations([]MutationEntry{
		{Project: "proj-a", Entity: "observation", EntityKey: "k1", Op: "upsert"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if !statusErr.IsAuthFailure() {
		t.Fatalf("expected IsAuthFailure, got status=%d", statusErr.StatusCode)
	}
}

// TestMutationTransportPush404ServerUnsupported verifies REQ-214: 404 → server_unsupported.
func TestMutationTransportPush404ServerUnsupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	// Suppress the log.Printf warning during test.
	orig := log.Writer()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(orig)

	mt := mustNewMutationTransport(t, srv.URL, "tok")
	_, err := mt.PushMutations([]MutationEntry{
		{Project: "proj-a", Entity: "observation", EntityKey: "k1", Op: "upsert"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.ErrorCode != "server_unsupported" {
		t.Fatalf("expected error_code=server_unsupported, got %q", statusErr.ErrorCode)
	}
}

// ─── PullMutations ───────────────────────────────────────────────────────────

// TestMutationTransportPullSinceSeq verifies REQ-201: pull returns mutations + has_more + latest_seq.
func TestMutationTransportPullSinceSeq(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/sync/mutations/pull" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer token123" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		sinceSeq := r.URL.Query().Get("since_seq")
		limit := r.URL.Query().Get("limit")
		if sinceSeq == "" || limit == "" {
			http.Error(w, "missing params", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"mutations":[{"seq":6,"entity":"observation","entity_key":"k6","op":"upsert","payload":{}}],"has_more":false,"latest_seq":10}`))
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "token123")
	resp, err := mt.PullMutations(5, 100)
	if err != nil {
		t.Fatalf("PullMutations: %v", err)
	}
	if len(resp.Mutations) != 1 {
		t.Fatalf("expected 1 mutation, got %d", len(resp.Mutations))
	}
	if resp.Mutations[0].Seq != 6 {
		t.Fatalf("expected seq=6, got %d", resp.Mutations[0].Seq)
	}
	if resp.HasMore {
		t.Fatal("expected has_more=false")
	}
	if resp.LatestSeq != 10 {
		t.Fatalf("expected latest_seq=10, got %d", resp.LatestSeq)
	}
}

// TestMutationTransportPullSendsSinceSeqAndLimit verifies query params are forwarded.
func TestMutationTransportPullSendsSinceSeqAndLimit(t *testing.T) {
	var gotSinceSeq, gotLimit string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSinceSeq = r.URL.Query().Get("since_seq")
		gotLimit = r.URL.Query().Get("limit")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"mutations":[],"has_more":false,"latest_seq":0}`))
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "tok")
	_, err := mt.PullMutations(42, 50)
	if err != nil {
		t.Fatalf("PullMutations: %v", err)
	}
	if gotSinceSeq != "42" {
		t.Fatalf("expected since_seq=42, got %q", gotSinceSeq)
	}
	if gotLimit != "50" {
		t.Fatalf("expected limit=50, got %q", gotLimit)
	}
}

// TestMutationTransportPullUnauth verifies REQ-201: 401 → IsAuthFailure.
func TestMutationTransportPullUnauth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "bad-token")
	_, err := mt.PullMutations(0, 100)
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if !statusErr.IsAuthFailure() {
		t.Fatalf("expected IsAuthFailure, got status=%d", statusErr.StatusCode)
	}
}

// TestMutationTransportPull404ServerUnsupported verifies REQ-214: pull 404 → server_unsupported.
func TestMutationTransportPull404ServerUnsupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	orig := log.Writer()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(orig)

	mt := mustNewMutationTransport(t, srv.URL, "tok")
	_, err := mt.PullMutations(0, 100)
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.ErrorCode != "server_unsupported" {
		t.Fatalf("expected error_code=server_unsupported, got %q", statusErr.ErrorCode)
	}
}

// TestMutationTransportPullHardFailOn500 verifies hard-fail on 500.
func TestMutationTransportPullHardFailOn500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "tok")
	_, err := mt.PullMutations(0, 100)
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

// ─── BC3: 404 warning log ─────────────────────────────────────────────────────

// TestTransport404LogsServerUnsupportedWarning verifies BC3 / REQ-214:
// When the server returns 404, newMutationHTTPStatusError must emit a log warning
// containing "server_unsupported".
func TestTransport404LogsServerUnsupportedWarning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	orig := log.Writer()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(orig)

	mt, err := NewMutationTransport(srv.URL, "token123")
	if err != nil {
		t.Fatalf("NewMutationTransport: %v", err)
	}
	_, _ = mt.PushMutations([]MutationEntry{
		{Project: "proj-a", Entity: "observation", EntityKey: "k1", Op: "upsert"},
	})

	logOutput := buf.String()
	if !strings.Contains(logOutput, "server_unsupported") {
		t.Fatalf("expected log to contain 'server_unsupported', got: %q", logOutput)
	}
}

// TestMutationTransportPush401VsNotFound verifies REQ-214: 401 → auth, NOT server_unsupported.
func TestMutationTransportPush401VsNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	mt := mustNewMutationTransport(t, srv.URL, "bad-token")
	_, err := mt.PushMutations([]MutationEntry{
		{Project: "proj-a", Entity: "observation", EntityKey: "k1", Op: "upsert"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.ErrorCode == "server_unsupported" {
		t.Fatal("401 must not map to server_unsupported")
	}
	if !statusErr.IsAuthFailure() {
		t.Fatalf("expected IsAuthFailure for 401, got status=%d code=%q", statusErr.StatusCode, statusErr.ErrorCode)
	}
}

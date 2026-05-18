// Package cloud — MutationTransport handles push/pull of fine-grained mutations
// to the cloud server. Unlike HTTPTransport (which handles chunk-level sync),
// MutationTransport operates on the mutation journal and supports cursor-based
// pull via since_seq.
//
// Architecture choice: Option B — separate MutationTransport struct.
//
// Rationale: upstream's internal/cloud/remote/transport.go defines MutationTransport
// as a distinct struct (not embedded in RemoteTransport). The sync.Transport interface
// defines only the 4 chunk methods (ReadManifest, WriteManifest, WriteChunk, ReadChunk).
// Extending that interface would break FileTransport and any future transport. A separate
// struct (and optional MutationTransporter interface) keeps the chunk contract clean and
// lets FileTransport opt out naturally — it does not implement MutationTransporter.
package cloud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ─── Types ────────────────────────────────────────────────────────────────────

// MutationEntry is a single fine-grained mutation to push to the cloud.
// It maps directly to the cloud server's mutation push payload.
type MutationEntry struct {
	Project   string          `json:"project"`
	Entity    string          `json:"entity"`   // "session" | "observation" | "prompt" | "relation"
	EntityKey string          `json:"entity_key"`
	Op        string          `json:"op"`      // "upsert" | "delete"
	Payload   json.RawMessage `json:"payload"`
}

// PulledMutation is a single mutation received from the cloud server during a pull.
type PulledMutation struct {
	Seq        int64           `json:"seq"`
	Entity     string          `json:"entity"`
	EntityKey  string          `json:"entity_key"`
	Op         string          `json:"op"`
	Payload    json.RawMessage `json:"payload"`
	OccurredAt string          `json:"occurred_at"`
}

// PullMutationsResponse is the response envelope returned by PullMutations.
type PullMutationsResponse struct {
	Mutations []PulledMutation `json:"mutations"`
	HasMore   bool             `json:"has_more"`
	LatestSeq int64            `json:"latest_seq"`
}

// ─── MutationTransporter interface ───────────────────────────────────────────

// MutationTransporter is the optional interface for transports that support
// fine-grained mutation push/pull. FileTransport does NOT implement this;
// only MutationTransport does.
//
// Callers that need mutation transport should type-assert to MutationTransporter:
//
//	if mt, ok := transport.(cloud.MutationTransporter); ok {
//	    seqs, err := mt.PushMutations(entries)
//	}
type MutationTransporter interface {
	PushMutations(entries []MutationEntry) ([]int64, error)
	PullMutations(sinceSeq int64, limit int) (*PullMutationsResponse, error)
}

// ─── MutationTransport ───────────────────────────────────────────────────────

// MutationTransport handles push/pull of fine-grained mutations to the cloud
// server. It satisfies the MutationTransporter interface.
type MutationTransport struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewMutationTransport creates a MutationTransport. baseURL must be a valid
// http/https URL; it is validated with validateBaseURL (same as HTTPTransport).
func NewMutationTransport(baseURL, token string) (*MutationTransport, error) {
	normalized, err := validateBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	return &MutationTransport{
		baseURL: normalized,
		token:   strings.TrimSpace(token),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (mt *MutationTransport) setAuthorization(req *http.Request) {
	if mt.token != "" {
		req.Header.Set("Authorization", "Bearer "+mt.token)
	}
}

// PushMutations POSTs a batch of mutations to POST /sync/mutations/push.
// Returns the list of accepted sequence numbers on success.
//
// Error semantics:
//   - 401 → HTTPStatusError.IsAuthFailure() == true
//   - 404 → HTTPStatusError.ErrorCode == "server_unsupported" + operator log warning
//   - Any other non-200 → hard-fail HTTPStatusError
func (mt *MutationTransport) PushMutations(entries []MutationEntry) ([]int64, error) {
	body, err := json.Marshal(map[string]any{"entries": entries})
	if err != nil {
		return nil, fmt.Errorf("cloud: marshal mutation push: %w", err)
	}

	reqURL := mt.baseURL + "/sync/mutations/push"
	req, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("cloud: build mutation push request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	mt.setAuthorization(req)

	resp, err := mt.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloud: mutation push: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, newMutationHTTPStatusError("mutation push", resp.StatusCode, respBody)
	}

	var result struct {
		AcceptedSeqs []int64 `json:"accepted_seqs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("cloud: decode mutation push response: %w", err)
	}
	return result.AcceptedSeqs, nil
}

// PullMutations fetches mutations from GET /sync/mutations/pull?since_seq=...&limit=...
// Returns mutations, a has_more flag, and the latest sequence number on the server.
//
// Error semantics:
//   - 401 → HTTPStatusError.IsAuthFailure() == true
//   - 404 → HTTPStatusError.ErrorCode == "server_unsupported" + operator log warning
//   - Any other non-200 → hard-fail HTTPStatusError
func (mt *MutationTransport) PullMutations(sinceSeq int64, limit int) (*PullMutationsResponse, error) {
	reqURL := fmt.Sprintf("%s/sync/mutations/pull?since_seq=%d&limit=%d", mt.baseURL, sinceSeq, limit)
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("cloud: build mutation pull request: %w", err)
	}
	mt.setAuthorization(req)

	resp, err := mt.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloud: mutation pull: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, newMutationHTTPStatusError("mutation pull", resp.StatusCode, respBody)
	}

	var result PullMutationsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("cloud: decode mutation pull response: %w", err)
	}
	return &result, nil
}

// ─── Error helpers ────────────────────────────────────────────────────────────

// newMutationHTTPStatusError creates an HTTPStatusError for mutation transport
// operations. 404 responses are mapped to ErrorCode="server_unsupported" and
// emit an operator-visible log warning (deploy the server first).
func newMutationHTTPStatusError(operation string, statusCode int, body []byte) error {
	var payload struct {
		ErrorClass string `json:"error_class"`
		ErrorCode  string `json:"error_code"`
		Error      string `json:"error"`
	}
	message := strings.TrimSpace(string(body))
	if err := json.Unmarshal(body, &payload); err == nil {
		if msg := strings.TrimSpace(payload.Error); msg != "" {
			message = msg
		}
	}

	errorCode := strings.TrimSpace(payload.ErrorCode)
	if statusCode == http.StatusNotFound {
		errorCode = "server_unsupported"
		log.Printf("[autosync] cloud mutation endpoint returned 404 (server_unsupported); deploy the new server first before enabling ENGRAM_CLOUD_AUTOSYNC=1")
	}

	return &HTTPStatusError{
		Operation:  operation,
		StatusCode: statusCode,
		ErrorClass: strings.TrimSpace(payload.ErrorClass),
		ErrorCode:  errorCode,
		Body:       message,
	}
}

// ─── FileTransport MutationTransport note ─────────────────────────────────────
//
// FileTransport (internal/sync/transport.go) does NOT implement MutationTransporter.
// Mutation push/pull requires a live cloud server; there is no meaningful local-file
// equivalent. Callers must check for the interface before calling push/pull:
//
//	if mt, ok := transport.(cloud.MutationTransporter); ok { ... }
//
// The autosync loop (PR #11) will enforce this check. For this PR we ship only the
// transport plumbing. No wiring to the mutation queue is added here — that is PR #11.

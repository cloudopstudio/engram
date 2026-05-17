// Package cloud provides the HTTP transport for cloud sync.
//
// HTTPTransport implements internal/sync.Transport using plain net/http calls
// to a remote Engram cloud server. It handles the 4-method interface:
//
//   - ReadManifest — GET /sync/pull?project=...
//   - WriteManifest — no-op (server owns the manifest)
//   - WriteChunk — POST /sync/push (JSON body with canonicalized chunk)
//   - ReadChunk — GET /sync/pull/{chunkID}?project=...
//
// All non-2xx responses are returned as *HTTPStatusError; the caller receives
// a hard failure (no silent fallback to FileTransport).
package cloud

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Gentleman-Programming/engram/internal/cloud/chunkcodec"
	"github.com/Gentleman-Programming/engram/internal/store"
	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
)

// HTTPStatusError is returned when the remote server responds with a
// non-2xx status code.
type HTTPStatusError struct {
	Operation  string
	StatusCode int
	ErrorClass string
	ErrorCode  string
	Body       string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("cloud: %s: status %d: %s", e.Operation, e.StatusCode, strings.TrimSpace(e.Body))
}

// IsAuthFailure reports whether the error is a 401 Unauthorized.
func (e *HTTPStatusError) IsAuthFailure() bool {
	return e != nil && e.StatusCode == http.StatusUnauthorized
}

// IsPolicyFailure reports whether the error is a 403 Forbidden.
func (e *HTTPStatusError) IsPolicyFailure() bool {
	return e != nil && e.StatusCode == http.StatusForbidden
}

// IsRepairableMigrationFailure reports whether the server flagged this as a
// repairable migration error (error_class == "repairable").
func (e *HTTPStatusError) IsRepairableMigrationFailure() bool {
	return e != nil && strings.TrimSpace(strings.ToLower(e.ErrorClass)) == "repairable"
}

// IsRepairable is an alias for IsRepairableMigrationFailure.
func (e *HTTPStatusError) IsRepairable() bool {
	return e.IsRepairableMigrationFailure()
}

func newHTTPStatusError(operation string, statusCode int, body []byte) error {
	errorClass := ""
	errorCode := ""
	message := strings.TrimSpace(string(body))
	var payload struct {
		ErrorClass string `json:"error_class"`
		ErrorCode  string `json:"error_code"`
		Error      string `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		errorClass = strings.TrimSpace(payload.ErrorClass)
		errorCode = strings.TrimSpace(payload.ErrorCode)
		if msg := strings.TrimSpace(payload.Error); msg != "" {
			message = msg
		}
	}
	return &HTTPStatusError{
		Operation:  operation,
		StatusCode: statusCode,
		ErrorClass: errorClass,
		ErrorCode:  errorCode,
		Body:       message,
	}
}

// ─── HTTPTransport ────────────────────────────────────────────────────────────

// HTTPTransport implements sync.Transport over HTTP. It satisfies the 4-method
// Transport interface and is the only cloud transport in this release.
// MutationTransport (fine-grained mutation push/pull) is deferred.
type HTTPTransport struct {
	baseURL    string
	token      string
	project    string
	httpClient *http.Client
}

// NewHTTPTransport creates an HTTPTransport for the given remote base URL,
// bearer token, and project name. Returns an error if the URL is invalid or
// the project name is empty after normalization.
func NewHTTPTransport(baseURL, token, project string) (*HTTPTransport, error) {
	normalized, err := validateBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	project, _ = store.NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, fmt.Errorf("cloud: project is required")
	}
	return &HTTPTransport{
		baseURL: normalized,
		token:   strings.TrimSpace(token),
		project: project,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func validateBaseURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("cloud: remote url is required")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("cloud: invalid remote url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("cloud: invalid remote url: scheme must be http or https")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("cloud: invalid remote url: host is required")
	}
	if strings.TrimSpace(parsed.RawQuery) != "" {
		return "", fmt.Errorf("cloud: invalid remote url: query is not allowed")
	}
	if strings.TrimSpace(parsed.Fragment) != "" {
		return "", fmt.Errorf("cloud: invalid remote url: fragment is not allowed")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

func (ht *HTTPTransport) endpointURL(query url.Values, parts ...string) (string, error) {
	endpoint, err := url.JoinPath(ht.baseURL, parts...)
	if err != nil {
		return "", fmt.Errorf("cloud: build request url: %w", err)
	}
	if len(query) == 0 {
		return endpoint, nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("cloud: build request url: %w", err)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (ht *HTTPTransport) setAuthorization(req *http.Request) {
	if ht.token == "" {
		return
	}
	req.Header.Set("Authorization", "Bearer "+ht.token)
}

// ReadManifest fetches the current manifest from GET /sync/pull?project=...
// Returns an HTTPStatusError for any non-200 response (hard-fail, no fallback).
func (ht *HTTPTransport) ReadManifest() (*engramsync.Manifest, error) {
	reqURL, err := ht.endpointURL(url.Values{"project": []string{ht.project}}, "sync", "pull")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("cloud: build manifest request: %w", err)
	}
	ht.setAuthorization(req)

	resp, err := ht.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloud: fetch manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, newHTTPStatusError("fetch manifest", resp.StatusCode, body)
	}

	var m engramsync.Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("cloud: parse manifest: %w", err)
	}
	return &m, nil
}

// WriteManifest is a no-op for the remote transport: the cloud server owns
// the manifest and updates it when a chunk is pushed successfully.
func (ht *HTTPTransport) WriteManifest(_ *engramsync.Manifest) error {
	return nil
}

// WriteChunk canonicalizes the chunk for the project and POSTs it to
// /sync/push. Returns an HTTPStatusError for any non-200 response.
func (ht *HTTPTransport) WriteChunk(chunkID string, data []byte, entry engramsync.ChunkEntry) error {
	canonicalData, err := chunkcodec.CanonicalizeForProject(data, ht.project)
	if err != nil {
		return fmt.Errorf("cloud: canonicalize push chunk: %w", err)
	}
	canonicalChunkID := chunkcodec.ChunkID(canonicalData)
	if strings.TrimSpace(chunkID) != "" && strings.TrimSpace(chunkID) != canonicalChunkID {
		chunkID = canonicalChunkID
	}

	body, err := json.Marshal(map[string]any{
		"chunk_id":          canonicalChunkID,
		"created_by":        entry.CreatedBy,
		"client_created_at": strings.TrimSpace(entry.CreatedAt),
		"project":           ht.project,
		"data":              json.RawMessage(canonicalData),
	})
	if err != nil {
		return fmt.Errorf("cloud: marshal push request: %w", err)
	}
	pushURL, err := ht.endpointURL(nil, "sync", "push")
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, pushURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cloud: build push request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	ht.setAuthorization(req)

	resp, err := ht.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cloud: push chunk %s: %w", chunkID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return newHTTPStatusError(fmt.Sprintf("push chunk %s", chunkID), resp.StatusCode, respBody)
	}
	return nil
}

// ReadChunk fetches a single chunk from GET /sync/pull/{chunkID}?project=...
// Returns ErrChunkNotFound for 404, HTTPStatusError for other non-200 responses.
func (ht *HTTPTransport) ReadChunk(chunkID string) ([]byte, error) {
	reqURL, err := ht.endpointURL(url.Values{"project": []string{ht.project}}, "sync", "pull", chunkID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("cloud: build pull request: %w", err)
	}
	ht.setAuthorization(req)
	resp, err := ht.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloud: pull chunk %s: %w", chunkID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, engramsync.ErrChunkNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, newHTTPStatusError(fmt.Sprintf("pull chunk %s", chunkID), resp.StatusCode, body)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cloud: read chunk %s response: %w", chunkID, err)
	}
	if len(data) == 0 {
		return nil, errors.New("cloud: empty chunk payload")
	}
	return data, nil
}

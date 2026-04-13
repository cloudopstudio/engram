//go:build pgstore

package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Gentleman-Programming/engram/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// pgTokenScope is the Azure AD scope for Azure Database for PostgreSQL.
	pgTokenScope = "https://ossrdbms-aad.database.windows.net/.default"

	// tokenRefreshBuffer is how far before expiry to refresh the token.
	tokenRefreshBuffer = 5 * time.Minute
)

// TokenProvider acquires and refreshes Entra ID access tokens for
// Azure Database for PostgreSQL authentication. Thread-safe.
type TokenProvider struct {
	cred    azcore.TokenCredential
	scope   string
	token   *azcore.AccessToken
	dataDir string // for file-based token cache persistence (empty = no persistence)
	mu      sync.RWMutex
}

// NewTokenProvider creates a TokenProvider using DefaultAzureCredential.
// This supports: Azure CLI (az login), managed identity, environment variables,
// and Visual Studio Code credentials.
func NewTokenProvider() (*TokenProvider, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("entra: create credential: %w\nRun 'az login' or configure a managed identity", err)
	}
	return &TokenProvider{
		cred:  cred,
		scope: pgTokenScope,
	}, nil
}

// ─── File-based token cache ──────────────────────────────────────────────────
// Replaces the azidentity/cache package which requires CGO for OS-native
// credential stores (macOS Keychain, Windows Credential Manager, Linux
// libsecret). This file-based cache works everywhere with CGO_ENABLED=0.

const tokenCacheFile = "token-cache.json"

// cachedToken is the on-disk format for the file-based token cache.
type cachedToken struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	ExpiresOn    time.Time `json:"expires_on"`
	TenantID     string    `json:"tenant_id,omitempty"`
	ClientID     string    `json:"client_id,omitempty"`
}

// tokenCachePath returns the full path to the token cache file.
func tokenCachePath(dataDir string) string {
	return filepath.Join(dataDir, tokenCacheFile)
}

// loadCachedToken reads and parses the token cache file.
// Returns nil (no error) if the file doesn't exist or is corrupted.
func loadCachedToken(dataDir string) (*cachedToken, error) {
	data, err := os.ReadFile(tokenCachePath(dataDir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read token cache: %w", err)
	}
	var ct cachedToken
	if err := json.Unmarshal(data, &ct); err != nil {
		// Corrupted cache — treat as missing.
		return nil, nil //nolint:nilerr
	}
	return &ct, nil
}

// saveCachedToken writes the token to the cache file with 0600 permissions.
// Creates the dataDir if it doesn't exist.
// refreshToken, tenantID and clientID are optional — pass empty string to omit.
func saveCachedToken(dataDir, accessToken, refreshToken, tenantID, clientID string, expiresOn time.Time) error {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return fmt.Errorf("create data dir for token cache: %w", err)
	}
	ct := cachedToken{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresOn:    expiresOn,
		TenantID:     tenantID,
		ClientID:     clientID,
	}
	data, err := json.Marshal(ct)
	if err != nil {
		return fmt.Errorf("marshal token cache: %w", err)
	}
	if err := os.WriteFile(tokenCachePath(dataDir), data, 0600); err != nil {
		return fmt.Errorf("write token cache: %w", err)
	}
	return nil
}

// ValidateCachedToken checks if a usable cached token exists.
// A token is usable if it is either:
//   - Still valid (not within tokenRefreshBuffer of expiry), OR
//   - Expired but has a refresh token that can be used to renew it.
//
// Returns nil if usable, error explaining what to do if not.
// The profile parameter is included in the error message to guide the user.
func ValidateCachedToken(dataDir, profile string) error {
	token, err := loadCachedToken(dataDir)
	if err != nil || token == nil {
		return fmt.Errorf("no cached Azure token found. Run:\n\n  engram login%s\n\nIf using OpenCode, the azure-entra-auth plugin handles this automatically.", profileFlag(profile))
	}
	// Valid (non-expired) token — OK.
	if time.Until(token.ExpiresOn) > tokenRefreshBuffer {
		return nil
	}
	// Expired but refresh token present — can be renewed silently.
	if token.RefreshToken != "" && token.TenantID != "" && token.ClientID != "" {
		return nil
	}
	return fmt.Errorf("cached Azure token has expired. Run:\n\n  engram login%s\n\nIf using OpenCode, the azure-entra-auth plugin handles this automatically.", profileFlag(profile))
}

// ValidateCachedTokenExported exposes ValidateCachedToken for non-pgstore callers.
func ValidateCachedTokenExported(dataDir, profile string) error {
	return ValidateCachedToken(dataDir, profile)
}

// profileFlag returns " --profile <name>" if profile is non-empty, or "".
func profileFlag(profile string) string {
	if profile == "" {
		return ""
	}
	return " --profile " + profile
}

// staticCredential implements azcore.TokenCredential for pre-cached tokens.
type staticCredential struct {
	token     string
	expiresOn time.Time
}

func (s *staticCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: s.token, ExpiresOn: s.expiresOn}, nil
}

// ─── Refreshable credential ──────────────────────────────────────────────────

// refreshableCredential implements azcore.TokenCredential and can silently
// renew an expired access token using the cached refresh token via a direct
// OAuth2 refresh_token HTTP grant. No CGO required.
type refreshableCredential struct {
	accessToken  string
	refreshToken string
	expiresOn    time.Time
	tenantID     string
	clientID     string
	scope        string
	dataDir      string
	mu           sync.Mutex
}

// GetToken returns a valid access token, refreshing via the refresh token when
// the access token is expired or within tokenRefreshBuffer of expiry.
func (r *refreshableCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if time.Until(r.expiresOn) > tokenRefreshBuffer {
		return azcore.AccessToken{Token: r.accessToken, ExpiresOn: r.expiresOn}, nil
	}

	if r.refreshToken == "" {
		return azcore.AccessToken{}, fmt.Errorf("access token expired and no refresh token available — run 'engram login' to re-authenticate")
	}

	newAccess, newRefresh, newExpiry, err := refreshAccessToken(r.tenantID, r.clientID, r.refreshToken, r.scope)
	if err != nil {
		return azcore.AccessToken{}, fmt.Errorf("silent token refresh failed: %w — run 'engram login' to re-authenticate", err)
	}

	r.accessToken = newAccess
	r.refreshToken = newRefresh
	r.expiresOn = newExpiry

	// Persist the new token pair (best-effort).
	if r.dataDir != "" {
		if saveErr := saveCachedToken(r.dataDir, newAccess, newRefresh, r.tenantID, r.clientID, newExpiry); saveErr != nil {
			fmt.Fprintf(os.Stderr, "engram: warning: failed to persist refreshed token: %v\n", saveErr)
		}
	}

	return azcore.AccessToken{Token: r.accessToken, ExpiresOn: r.expiresOn}, nil
}

// ─── Direct OAuth2 HTTP helpers ──────────────────────────────────────────────

// tokenEndpoint returns the Azure OAuth2 v2 token endpoint for a tenant.
func tokenEndpoint(tenantID string) string {
	return fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", tenantID)
}

// azureTokenResponse is the JSON shape returned by Azure token endpoints.
type azureTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Error        string `json:"error"`
	ErrorDesc    string `json:"error_description"`
}

// refreshAccessToken exchanges a refresh token for a new access+refresh token
// pair by making a direct OAuth2 HTTP POST. No third-party dependencies required.
func refreshAccessToken(tenantID, clientID, refreshToken, scope string) (accessToken, newRefreshToken string, expiresOn time.Time, err error) {
	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("client_id", clientID)
	data.Set("refresh_token", refreshToken)
	data.Set("scope", scope+" offline_access")

	resp, err := http.PostForm(tokenEndpoint(tenantID), data)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("http post to token endpoint: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("read token response: %w", err)
	}

	var tr azureTokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", "", time.Time{}, fmt.Errorf("parse token response: %w", err)
	}
	if tr.Error != "" {
		return "", "", time.Time{}, fmt.Errorf("azure token error %s: %s", tr.Error, tr.ErrorDesc)
	}
	if tr.AccessToken == "" {
		return "", "", time.Time{}, fmt.Errorf("azure returned empty access token (status %d)", resp.StatusCode)
	}

	expiry := time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	return tr.AccessToken, tr.RefreshToken, expiry, nil
}

// deviceCodeResponse is the JSON shape returned by the device code endpoint.
type deviceCodeResponse struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int64  `json:"expires_in"`
	Interval        int64  `json:"interval"`
	Message         string `json:"message"`
	Error           string `json:"error"`
	ErrorDesc       string `json:"error_description"`
}

// deviceCodeFlow performs the full Azure Device Code Flow using direct HTTP
// calls. This gives us full control over the token response, including the
// refresh token (which azidentity.DeviceCodeCredential does not expose).
//
// It writes the user prompt to stderr and blocks until the user completes
// authentication in their browser or the device code expires.
//
// Returns the access token, refresh token, and expiry time on success.
func deviceCodeFlow(ctx context.Context, tenantID, clientID, scope string) (accessToken, refreshToken string, expiresOn time.Time, err error) {
	// Step 1: Request device + user code.
	dcEndpoint := fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/devicecode", tenantID)
	dcData := url.Values{}
	dcData.Set("client_id", clientID)
	dcData.Set("scope", scope+" offline_access")

	dcResp, err := http.PostForm(dcEndpoint, dcData)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("request device code: %w", err)
	}
	defer dcResp.Body.Close()

	dcBody, err := io.ReadAll(dcResp.Body)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("read device code response: %w", err)
	}

	var dc deviceCodeResponse
	if err := json.Unmarshal(dcBody, &dc); err != nil {
		return "", "", time.Time{}, fmt.Errorf("parse device code response: %w", err)
	}
	if dc.Error != "" {
		return "", "", time.Time{}, fmt.Errorf("device code error %s: %s", dc.Error, dc.ErrorDesc)
	}
	if dc.DeviceCode == "" {
		return "", "", time.Time{}, fmt.Errorf("azure returned empty device code (status %d)", dcResp.StatusCode)
	}

	// Display the user prompt.
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "  ┌─────────────────────────────────────────────────┐\n")
	fmt.Fprintf(os.Stderr, "  │  engram: Azure authentication required           │\n")
	fmt.Fprintf(os.Stderr, "  │                                                  │\n")
	fmt.Fprintf(os.Stderr, "  │  %s\n", dc.Message)
	fmt.Fprintf(os.Stderr, "  │                                                  │\n")
	fmt.Fprintf(os.Stderr, "  │  Waiting for authentication...                   │\n")
	fmt.Fprintf(os.Stderr, "  └─────────────────────────────────────────────────┘\n")
	fmt.Fprintf(os.Stderr, "\n")

	// Step 2: Poll until user authenticates or device code expires.
	interval := time.Duration(dc.Interval) * time.Second
	if interval < time.Second {
		interval = 5 * time.Second
	}

	tokenData := url.Values{}
	tokenData.Set("grant_type", "urn:ietf:params:oauth:grant-type:device_code")
	tokenData.Set("client_id", clientID)
	tokenData.Set("device_code", dc.DeviceCode)

	deadline := time.Now().Add(time.Duration(dc.ExpiresIn) * time.Second)
	for {
		select {
		case <-ctx.Done():
			return "", "", time.Time{}, ctx.Err()
		case <-time.After(interval):
		}

		if time.Now().After(deadline) {
			return "", "", time.Time{}, fmt.Errorf("device code expired — run 'engram login' to try again")
		}

		pollResp, err := http.PostForm(tokenEndpoint(tenantID), tokenData)
		if err != nil {
			return "", "", time.Time{}, fmt.Errorf("poll token endpoint: %w", err)
		}
		pollBody, err := io.ReadAll(pollResp.Body)
		pollResp.Body.Close()
		if err != nil {
			return "", "", time.Time{}, fmt.Errorf("read poll response: %w", err)
		}

		var tr azureTokenResponse
		if err := json.Unmarshal(pollBody, &tr); err != nil {
			return "", "", time.Time{}, fmt.Errorf("parse poll response: %w", err)
		}

		switch tr.Error {
		case "":
			// Success.
			if tr.AccessToken == "" {
				return "", "", time.Time{}, fmt.Errorf("azure returned empty access token (status %d)", pollResp.StatusCode)
			}
			expiry := time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
			return tr.AccessToken, tr.RefreshToken, expiry, nil
		case "authorization_pending":
			// User hasn't completed auth yet — keep polling.
			continue
		case "slow_down":
			// Azure asking us to slow down.
			interval += 5 * time.Second
			continue
		case "authorization_declined":
			return "", "", time.Time{}, fmt.Errorf("authentication declined by user")
		case "expired_token":
			return "", "", time.Time{}, fmt.Errorf("device code expired — run 'engram login' to try again")
		default:
			return "", "", time.Time{}, fmt.Errorf("azure token error %s: %s", tr.Error, tr.ErrorDesc)
		}
	}
}

// NewDeviceCodeTokenProvider creates a TokenProvider that uses Azure Device Code
// Flow with a file-based token cache. This is intended for non-dev users who
// can't use 'az login' (e.g., running inside OpenCode/Claude/etc.).
//
// The file-based cache stores the access token AND refresh token in
// ~/.engram/token-cache.json (0600 permissions). On startup the resolution
// order is:
//
//  1. Valid (non-expired) cached access token → used directly, no prompt.
//  2. Expired cached token with a refresh token → silent renewal via OAuth2
//     refresh_token grant, no prompt.
//  3. No usable cache → interactive Device Code Flow prompt on stderr.
//
// ALL output goes to stderr — stdout is reserved for the MCP stdio protocol.
func NewDeviceCodeTokenProvider(tenantID, clientID, dataDir string) (*TokenProvider, error) {
	// Check file-based token cache first.
	if dataDir != "" {
		cached, err := loadCachedToken(dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "engram: warning: failed to read token cache: %v\n", err)
		}

		if cached != nil {
			// Case 1: access token still valid.
			if time.Until(cached.ExpiresOn) > tokenRefreshBuffer {
				return &TokenProvider{
					cred:    &staticCredential{token: cached.AccessToken, expiresOn: cached.ExpiresOn},
					scope:   pgTokenScope,
					dataDir: dataDir,
					token:   &azcore.AccessToken{Token: cached.AccessToken, ExpiresOn: cached.ExpiresOn},
				}, nil
			}

			// Case 2: access token expired but refresh token present.
			if cached.RefreshToken != "" && cached.TenantID != "" && cached.ClientID != "" {
				cred := &refreshableCredential{
					accessToken:  cached.AccessToken,
					refreshToken: cached.RefreshToken,
					expiresOn:    cached.ExpiresOn,
					tenantID:     cached.TenantID,
					clientID:     cached.ClientID,
					scope:        pgTokenScope,
					dataDir:      dataDir,
				}
				return &TokenProvider{
					cred:    cred,
					scope:   pgTokenScope,
					dataDir: dataDir,
				}, nil
			}
		}
	}

	// Case 3: No usable cache — perform interactive Device Code Flow.
	// We use direct HTTP calls so we can capture the refresh token,
	// which azidentity.DeviceCodeCredential does not expose.
	accessToken, refreshToken, expiresOn, err := deviceCodeFlow(context.Background(), tenantID, clientID, pgTokenScope)
	if err != nil {
		return nil, fmt.Errorf("entra: device code flow: %w", err)
	}

	// Persist both tokens immediately.
	if dataDir != "" {
		if saveErr := saveCachedToken(dataDir, accessToken, refreshToken, tenantID, clientID, expiresOn); saveErr != nil {
			fmt.Fprintf(os.Stderr, "engram: warning: failed to cache token: %v\n", saveErr)
		}
	}

	return &TokenProvider{
		cred:    &staticCredential{token: accessToken, expiresOn: expiresOn},
		scope:   pgTokenScope,
		dataDir: dataDir,
		token:   &azcore.AccessToken{Token: accessToken, ExpiresOn: expiresOn},
	}, nil
}

// newTokenProviderFromCache creates a TokenProvider from a cached token.
// If the token has a refresh token, it uses refreshableCredential so the
// access token can be silently renewed without user interaction.
// If the token is still valid, it uses staticCredential for simplicity.
func newTokenProviderFromCache(cached *cachedToken, dataDir string) *TokenProvider {
	// If access token still valid — use static (fast path, no refresh needed yet).
	if time.Until(cached.ExpiresOn) > tokenRefreshBuffer {
		return &TokenProvider{
			cred:    &staticCredential{token: cached.AccessToken, expiresOn: cached.ExpiresOn},
			scope:   pgTokenScope,
			dataDir: dataDir,
			token:   &azcore.AccessToken{Token: cached.AccessToken, ExpiresOn: cached.ExpiresOn},
		}
	}

	// Access token expired — use refreshable if we have a refresh token.
	if cached.RefreshToken != "" && cached.TenantID != "" && cached.ClientID != "" {
		cred := &refreshableCredential{
			accessToken:  cached.AccessToken,
			refreshToken: cached.RefreshToken,
			expiresOn:    cached.ExpiresOn,
			tenantID:     cached.TenantID,
			clientID:     cached.ClientID,
			scope:        pgTokenScope,
			dataDir:      dataDir,
		}
		return &TokenProvider{
			cred:    cred,
			scope:   pgTokenScope,
			dataDir: dataDir,
		}
	}

	// Fallback: static credential even though it may be expired.
	// ValidateCachedToken should have prevented us getting here without a
	// valid token or refresh token, but be defensive.
	return &TokenProvider{
		cred:    &staticCredential{token: cached.AccessToken, expiresOn: cached.ExpiresOn},
		scope:   pgTokenScope,
		dataDir: dataDir,
		token:   &azcore.AccessToken{Token: cached.AccessToken, ExpiresOn: cached.ExpiresOn},
	}
}

// resolveInteractiveAuth resolves tenant-id and client-id for device code flow
// using the resolution chain: env vars → config (with profile) → error.
func resolveInteractiveAuth(dataDir, profile string) (tenantID, clientID string, err error) {
	// 1. Environment variables
	tenantID = os.Getenv("AZURE_TENANT_ID")
	clientID = os.Getenv("AZURE_CLIENT_ID")

	// 2. Config file (with profile support)
	if tenantID == "" && dataDir != "" {
		if v, cfgErr := config.GetWithProfile(dataDir, profile, "tenant-id"); cfgErr == nil && v != "" {
			tenantID = v
		}
	}
	if clientID == "" && dataDir != "" {
		if v, cfgErr := config.GetWithProfile(dataDir, profile, "client-id"); cfgErr == nil && v != "" {
			clientID = v
		}
	}

	// 3. Validate both are present
	if tenantID == "" || clientID == "" {
		return "", "", fmt.Errorf("device code auth requires tenant-id and client-id.\n  Run: engram config set tenant-id <your-tenant-id>\n       engram config set client-id <your-client-id>")
	}

	return tenantID, clientID, nil
}

// Token returns a valid access token, refreshing if within tokenRefreshBuffer
// of expiry. Only one refresh request is made even with concurrent callers.
// After a successful refresh, the token is persisted to the file-based cache
// if dataDir is set.
func (tp *TokenProvider) Token(ctx context.Context) (string, error) {
	// Fast path: read lock, check cache.
	tp.mu.RLock()
	if tp.token != nil && time.Until(tp.token.ExpiresOn) > tokenRefreshBuffer {
		t := tp.token.Token
		tp.mu.RUnlock()
		return t, nil
	}
	tp.mu.RUnlock()

	// Slow path: write lock, double-check, refresh.
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if tp.token != nil && time.Until(tp.token.ExpiresOn) > tokenRefreshBuffer {
		return tp.token.Token, nil
	}

	token, err := tp.cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{tp.scope},
	})
	if err != nil {
		return "", fmt.Errorf("entra: get token: %w", err)
	}
	tp.token = &token

	// Persist to file cache (best-effort — don't fail the token request).
	// We preserve any existing refresh token, tenant, and client IDs since
	// TokenProvider.Token() is used for DefaultAzureCredential paths which
	// don't use the refresh token mechanism.
	if tp.dataDir != "" {
		var existingRefresh, existingTenant, existingClient string
		if existing, _ := loadCachedToken(tp.dataDir); existing != nil {
			existingRefresh = existing.RefreshToken
			existingTenant = existing.TenantID
			existingClient = existing.ClientID
		}
		if saveErr := saveCachedToken(tp.dataDir, token.Token, existingRefresh, existingTenant, existingClient, token.ExpiresOn); saveErr != nil {
			fmt.Fprintf(os.Stderr, "engram: warning: failed to cache token: %v\n", saveErr)
		}
	}

	return token.Token, nil
}

// Identity extracts the email/UPN from the cached token's JWT claims.
// Returns empty string if no token is cached or claims can't be parsed.
func (tp *TokenProvider) Identity() string {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	if tp.token == nil {
		return ""
	}

	// Parse the JWT payload (middle segment) without signature verification —
	// we trust the token since we got it from Azure.
	parts := strings.SplitN(tp.token.Token, ".", 3)
	if len(parts) < 2 {
		return ""
	}

	// JWT uses base64url encoding (no padding).
	payload := parts[1]
	// Add padding if needed.
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return ""
	}

	var claims struct {
		UPN               string `json:"upn"`
		PreferredUsername string `json:"preferred_username"`
		Email             string `json:"email"`
	}
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return ""
	}

	// Try fields in priority order.
	if claims.UPN != "" {
		return claims.UPN
	}
	if claims.PreferredUsername != "" {
		return claims.PreferredUsername
	}
	return claims.Email
}

// resolveAuthMethod determines the authentication method from env vars,
// config file (with profile support), and the connection string host.
// Returns "entra" or "password".
// dataDir is used to read config file fallback; empty string skips config lookup.
// profile is the active profile name (may be "").
func resolveAuthMethod(connStr string, dataDir string, profile string) string {
	if method := os.Getenv("ENGRAM_AUTH_METHOD"); method != "" {
		return strings.ToLower(strings.TrimSpace(method))
	}
	if dataDir != "" {
		if method, err := config.GetWithProfile(dataDir, profile, "auth-method"); err == nil && method != "" {
			return strings.ToLower(strings.TrimSpace(method))
		}
	}
	// Auto-detect: if the host is Azure, use Entra ID.
	if strings.Contains(connStr, ".database.azure.com") {
		return "entra"
	}
	return "password"
}

// configurePGPool creates a pgxpool.Config from a connection string, optionally
// injecting Entra ID tokens via the BeforeConnect hook and setting the
// engram.identity GUC variable via AfterConnect for RLS policies.
func configurePGPool(connStr string, tp *TokenProvider) (*pgxpool.Config, error) {
	pgxCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse pg connection string: %w", err)
	}

	pgxCfg.MinConns = 1
	pgxCfg.MaxConns = 5
	pgxCfg.MaxConnLifetime = 30 * time.Minute
	pgxCfg.MaxConnIdleTime = 5 * time.Minute
	pgxCfg.HealthCheckPeriod = 30 * time.Second

	// Statement timeout to protect against runaway queries.
	pgxCfg.ConnConfig.RuntimeParams["statement_timeout"] = "30000"

	// Inject Entra ID token before each new connection.
	if tp != nil {
		pgxCfg.BeforeConnect = func(ctx context.Context, cfg *pgx.ConnConfig) error {
			token, err := tp.Token(ctx)
			if err != nil {
				return fmt.Errorf("entra token for pg connection: %w", err)
			}
			cfg.Password = token
			return nil
		}

		// Set engram.identity GUC on each new connection so RLS policies
		// can identify the real user even when multiple Entra users share
		// the same PostgreSQL role. The identity is extracted from the JWT
		// token's UPN/email claim.
		pgxCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			identity := tp.Identity()
			if identity != "" {
				if _, err := conn.Exec(ctx,
					"SELECT set_config('engram.identity', $1, false)", identity,
				); err != nil {
					return fmt.Errorf("set engram.identity GUC: %w", err)
				}
			}
			return nil
		}
	}

	return pgxCfg, nil
}

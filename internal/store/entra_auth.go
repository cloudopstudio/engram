//go:build pgstore

package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	AccessToken string    `json:"access_token"`
	ExpiresOn   time.Time `json:"expires_on"`
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
func saveCachedToken(dataDir, accessToken string, expiresOn time.Time) error {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return fmt.Errorf("create data dir for token cache: %w", err)
	}
	ct := cachedToken{
		AccessToken: accessToken,
		ExpiresOn:   expiresOn,
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

// ValidateCachedToken checks if a valid (non-expired) cached token exists.
// Returns nil if valid, error explaining what to do if not.
// The profile parameter is included in the error message to guide the user.
func ValidateCachedToken(dataDir, profile string) error {
	token, err := loadCachedToken(dataDir)
	if err != nil || token == nil {
		return fmt.Errorf("no cached Azure token found. Run:\n\n  engram login%s\n\nIf using OpenCode, the azure-entra-auth plugin handles this automatically.", profileFlag(profile))
	}
	if time.Now().After(token.ExpiresOn.Add(-tokenRefreshBuffer)) {
		return fmt.Errorf("cached Azure token has expired. Run:\n\n  engram login%s\n\nIf using OpenCode, the azure-entra-auth plugin handles this automatically.", profileFlag(profile))
	}
	return nil
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

// NewDeviceCodeTokenProvider creates a TokenProvider that uses Azure Device Code
// Flow with a file-based token cache. This is intended for non-dev users who
// can't use 'az login' (e.g., running inside OpenCode/Claude/etc.).
//
// The file-based cache stores the access token in ~/.engram/token-cache.json
// (0600 permissions). On startup, if a valid (non-expired) cached token exists,
// it is used directly — no device code prompt. When expired or missing, a new
// device code flow is triggered and the cache is updated.
//
// ALL output goes to stderr — stdout is reserved for the MCP stdio protocol.
func NewDeviceCodeTokenProvider(tenantID, clientID, dataDir string) (*TokenProvider, error) {
	// Check file-based token cache first.
	if dataDir != "" {
		cached, err := loadCachedToken(dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "engram: warning: failed to read token cache: %v\n", err)
		}
		if cached != nil && time.Until(cached.ExpiresOn) > tokenRefreshBuffer {
			// Valid cached token — wrap in a static credential provider.
			return &TokenProvider{
				cred:    &staticCredential{token: cached.AccessToken, expiresOn: cached.ExpiresOn},
				scope:   pgTokenScope,
				dataDir: dataDir,
				token:   &azcore.AccessToken{Token: cached.AccessToken, ExpiresOn: cached.ExpiresOn},
			}, nil
		}
	}

	// No valid cache — do device code flow.
	cred, err := azidentity.NewDeviceCodeCredential(&azidentity.DeviceCodeCredentialOptions{
		TenantID: tenantID,
		ClientID: clientID,
		UserPrompt: func(ctx context.Context, msg azidentity.DeviceCodeMessage) error {
			fmt.Fprintf(os.Stderr, "\n")
			fmt.Fprintf(os.Stderr, "  ┌─────────────────────────────────────────────────┐\n")
			fmt.Fprintf(os.Stderr, "  │  engram: Azure authentication required           │\n")
			fmt.Fprintf(os.Stderr, "  │                                                  │\n")
			fmt.Fprintf(os.Stderr, "  │  %s\n", msg.Message)
			fmt.Fprintf(os.Stderr, "  │                                                  │\n")
			fmt.Fprintf(os.Stderr, "  │  Waiting for authentication...                   │\n")
			fmt.Fprintf(os.Stderr, "  └─────────────────────────────────────────────────┘\n")
			fmt.Fprintf(os.Stderr, "\n")
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("entra: create device code credential: %w", err)
	}

	return &TokenProvider{
		cred:    cred,
		scope:   pgTokenScope,
		dataDir: dataDir,
	}, nil
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
	if tp.dataDir != "" {
		if saveErr := saveCachedToken(tp.dataDir, token.Token, token.ExpiresOn); saveErr != nil {
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

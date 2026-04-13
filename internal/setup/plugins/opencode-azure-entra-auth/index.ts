/**
 * opencode-azure-entra-auth
 *
 * OpenCode plugin that authenticates with Azure Entra ID using
 * `@azure/identity` SDK. The SDK handles all OAuth2 complexity
 * (PKCE, callback server, browser, token exchange, caching).
 *
 * Saves tokens to ~/.engram/token-cache.json so engram's Go MCP
 * server can read them and connect to Azure Database for PostgreSQL.
 *
 * Auth hook flow:
 *   1. User runs `/connect` in OpenCode
 *   2. Selects "Azure Entra ID (engram database)"
 *   3. If tenant-id/client-id not configured, prompts for them
 *   4. `InteractiveBrowserCredential` opens browser + handles PKCE
 *   5. Token saved to token-cache.json
 *
 * Loader (startup):
 *   - Checks token-cache.json on each startup
 *   - If expired → uses `InteractiveBrowserCredential` to get fresh token
 *     (SDK handles token caching and silent refresh internally)
 *   - No manual refresh_token handling needed
 *
 * IMPORTANT: @azure/identity is lazy-loaded (dynamic import) to avoid
 * breaking module loading in OpenCode's embedded Bun runtime. The heavy
 * MSAL/JWT dependencies are only pulled in when authentication is needed.
 *
 * @license Apache-2.0
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { homedir } from "os";
import { join } from "path";

import type { Plugin, AuthOAuthResult, Hooks } from "@opencode-ai/plugin";

// ---- Constants ----

const PLUGIN_NAME = "opencode-azure-entra-auth";

/** Custom provider ID for engram database auth. */
const AZURE_ENGRAM_PROVIDER = "azure-engram";

/** Azure AD scope for Azure Database for PostgreSQL. */
const PG_TOKEN_SCOPE = "https://ossrdbms-aad.database.windows.net/.default";

/** Buffer before expiry to consider a token stale (5 minutes). */
const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000;

/** Default engram data directory. */
const ENGRAM_DATA_DIR = join(homedir(), ".engram");

/** Token cache filename (shared with engram's Go code). */
const TOKEN_CACHE_FILE = "token-cache.json";

/** Config filename (shared with engram's Go config package). */
const CONFIG_FILE = "config.json";

// ---- Types ----

/**
 * Token cache format. Backward-compatible with engram's Go code
 * which reads `access_token` and `expires_on`.
 */
interface TokenCache {
  access_token: string;
  expires_on: string; // ISO 8601 datetime
}

interface EngramConfig {
  "tenant-id"?: string;
  "client-id"?: string;
  "default-profile"?: string;
  profiles?: Record<string, Record<string, string>>;
  [key: string]: unknown;
}

// ---- Logging ----

function log(level: "info" | "warn" | "error", msg: string): void {
  const prefix = `[${PLUGIN_NAME}]`;
  if (level === "error") {
    console.error(`${prefix} ERROR: ${msg}`);
  } else if (level === "warn") {
    console.error(`${prefix} WARN: ${msg}`);
  } else {
    console.error(`${prefix} ${msg}`);
  }
}

// ---- Config Reading / Writing ----

function readEngramConfig(): EngramConfig {
  const configPath = join(ENGRAM_DATA_DIR, CONFIG_FILE);
  if (!existsSync(configPath)) return {};
  try {
    return JSON.parse(readFileSync(configPath, "utf-8"));
  } catch {
    log("warn", `Failed to parse ${configPath}`);
    return {};
  }
}

/**
 * Save values to engram config so `engram` CLI can also read them.
 * Merges with existing config -- does not overwrite unrelated keys.
 */
function saveEngramConfig(updates: Partial<EngramConfig>): void {
  const cfg = readEngramConfig();
  const merged = { ...cfg, ...updates };
  mkdirSync(ENGRAM_DATA_DIR, { recursive: true, mode: 0o700 });
  const configPath = join(ENGRAM_DATA_DIR, CONFIG_FILE);
  writeFileSync(configPath, JSON.stringify(merged, null, 2) + "\n", {
    mode: 0o600,
  });
}

/**
 * Resolve a config value with profile fallback:
 *   env var -> profile value -> root value -> undefined
 */
function resolveConfig(
  cfg: EngramConfig,
  key: string,
  envVar: string,
  profile?: string,
): string | undefined {
  const envVal = process.env[envVar];
  if (envVal) return envVal;

  const effectiveProfile = profile || cfg["default-profile"];

  if (effectiveProfile && cfg.profiles?.[effectiveProfile]?.[key]) {
    return cfg.profiles[effectiveProfile][key];
  }

  const rootVal = cfg[key];
  if (typeof rootVal === "string" && rootVal) return rootVal;

  return undefined;
}

// ---- Token Cache ----

function tokenCachePath(): string {
  return join(ENGRAM_DATA_DIR, TOKEN_CACHE_FILE);
}

function loadTokenCache(): TokenCache | null {
  const path = tokenCachePath();
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf-8"));
  } catch {
    log("warn", `Failed to parse ${path} -- treating as empty`);
    return null;
  }
}

function saveTokenCache(cache: TokenCache): void {
  mkdirSync(ENGRAM_DATA_DIR, { recursive: true, mode: 0o700 });
  writeFileSync(tokenCachePath(), JSON.stringify(cache, null, 2) + "\n", {
    mode: 0o600,
  });
}

function isTokenValid(cache: TokenCache | null): boolean {
  if (!cache?.access_token || !cache.expires_on) return false;
  const expiresOn = new Date(cache.expires_on).getTime();
  return Date.now() + TOKEN_REFRESH_BUFFER_MS < expiresOn;
}

// ---- Azure Identity Helpers ----

/**
 * Detect if running in a headless environment (SSH, CI, etc.).
 */
function isHeadless(): boolean {
  return !!(
    process.env.SSH_CONNECTION ||
    process.env.SSH_CLIENT ||
    process.env.SSH_TTY ||
    process.env.OPENCODE_HEADLESS
  );
}

/**
 * Lazy-load @azure/identity to avoid breaking module evaluation.
 * The MSAL/JWT dependencies are heavy and may not load in all
 * Bun runtime environments (e.g., OpenCode's embedded Bun).
 */
async function loadAzureIdentity() {
  return await import("@azure/identity");
}

/**
 * Get a token using `@azure/identity`. Uses InteractiveBrowserCredential
 * for desktop environments (opens browser automatically) or
 * DeviceCodeCredential for headless/SSH environments.
 *
 * The SDK handles internally:
 * - PKCE generation
 * - Local callback server
 * - Browser opening
 * - Token exchange
 * - In-memory token caching
 */
async function acquireToken(
  tenantId: string,
  clientId: string,
): Promise<TokenCache> {
  const { InteractiveBrowserCredential, DeviceCodeCredential } =
    await loadAzureIdentity();

  if (isHeadless()) {
    log("info", "Headless environment detected. Using device code flow.");
    const credential = new DeviceCodeCredential({
      tenantId,
      clientId,
      userPromptCallback: (info) => {
        log("info", info.message);
      },
    });
    const token = await credential.getToken(PG_TOKEN_SCOPE);
    return {
      access_token: token.token,
      expires_on: new Date(token.expiresOnTimestamp).toISOString(),
    };
  }

  const credential = new InteractiveBrowserCredential({
    tenantId,
    clientId,
    redirectUri: "http://localhost",
  });

  const token = await credential.getToken(PG_TOKEN_SCOPE);
  return {
    access_token: token.token,
    expires_on: new Date(token.expiresOnTimestamp).toISOString(),
  };
}

// ---- Credential Resolution ----

/**
 * Resolve tenant-id and client-id from:
 *   1. Prompt inputs (from /connect UI)
 *   2. Environment variables
 *   3. Engram config file
 */
function resolveCredentials(inputs?: Record<string, string>): {
  tenantId: string | undefined;
  clientId: string | undefined;
} {
  const cfg = readEngramConfig();
  const profile =
    process.env.ENGRAM_PROFILE || cfg["default-profile"] || undefined;

  const tenantId =
    inputs?.tenantId ||
    resolveConfig(cfg, "tenant-id", "AZURE_TENANT_ID", profile);
  const clientId =
    inputs?.clientId ||
    resolveConfig(cfg, "client-id", "AZURE_CLIENT_ID", profile);

  return { tenantId, clientId };
}

// ---- Plugin Entry Point ----

/**
 * OpenCode auth-hook plugin for Azure Entra ID.
 *
 * Registers as provider "azure-engram" and appears in `/connect`.
 * Uses `@azure/identity` SDK for all OAuth2 operations.
 *
 * Follows the same export pattern as opencode-anthropic-login-via-cli:
 * a bare async function as the default export (legacy plugin format).
 * This is the most widely compatible format with OpenCode's plugin loader.
 */
const plugin: Plugin = async (_input, _options) => {
  log("info", "Plugin initializing.");

  const hooks: Hooks = {
    auth: {
      provider: AZURE_ENGRAM_PROVIDER,

      /**
       * Loader: called on every startup when this provider has stored auth.
       *
       * Checks token-cache.json and refreshes if needed using @azure/identity.
       * Since this is for a DATABASE (not an LLM), the loader doesn't
       * intercept fetch -- it just ensures the token file is fresh.
       */
      async loader() {
        const { tenantId, clientId } = resolveCredentials();

        if (!tenantId || !clientId) {
          log(
            "info",
            "No tenant-id/client-id configured. Skipping token check.",
          );
          return null;
        }

        const cached = loadTokenCache();

        if (isTokenValid(cached)) {
          log("info", "Valid Azure token found in cache.");
          return null;
        }

        // Token expired or missing -- try to get a fresh one.
        log(
          "info",
          "Token expired or missing. Attempting to acquire fresh token...",
        );
        try {
          const freshToken = await acquireToken(tenantId, clientId);
          saveTokenCache(freshToken);
          log("info", "Token refreshed successfully.");
        } catch (err) {
          log(
            "warn",
            `Token acquisition failed: ${err instanceof Error ? err.message : String(err)}. ` +
              "User will need to re-authenticate via /connect.",
          );
        }

        return null;
      },

      methods: [
        {
          type: "oauth" as const,
          label: "Azure Entra ID (engram database)",

          /**
           * Prompts shown in the /connect UI if tenant-id or client-id
           * are not already configured. Values are saved to engram config
           * so the user only enters them once.
           */
          prompts: [
            {
              type: "text" as const,
              key: "tenantId",
              message: "Azure Tenant ID",
              placeholder: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
              validate: (value: string) => {
                if (!value.trim()) return "Tenant ID is required";
                if (
                  !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
                    value.trim(),
                  )
                ) {
                  return "Must be a valid UUID (e.g. xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)";
                }
                return undefined;
              },
            },
            {
              type: "text" as const,
              key: "clientId",
              message: "Azure App Client ID",
              placeholder: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
              validate: (value: string) => {
                if (!value.trim()) return "Client ID is required";
                if (
                  !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
                    value.trim(),
                  )
                ) {
                  return "Must be a valid UUID (e.g. yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy)";
                }
                return undefined;
              },
            },
          ],

          /**
           * authorize() -- called when user selects this method in /connect.
           *
           * Uses `@azure/identity` InteractiveBrowserCredential which handles
           * the entire OAuth2 flow: PKCE, local server, browser, token exchange.
           *
           * Returns "auto" method since the SDK manages the browser flow
           * internally. The callback triggers the actual authentication.
           */
          async authorize(
            inputs?: Record<string, string>,
          ): Promise<AuthOAuthResult> {
            const { tenantId, clientId } = resolveCredentials(inputs);

            if (!tenantId || !clientId) {
              throw new Error(
                "Missing tenant-id or client-id. " +
                  "Configure with: engram config set tenant-id <id> && engram config set client-id <id>",
              );
            }

            // Persist prompt inputs to engram config for future use.
            if (inputs?.tenantId || inputs?.clientId) {
              const updates: Partial<EngramConfig> = {};
              if (inputs.tenantId)
                updates["tenant-id"] = inputs.tenantId.trim();
              if (inputs.clientId)
                updates["client-id"] = inputs.clientId.trim();
              saveEngramConfig(updates);
              log("info", "Saved Azure credentials to engram config.");
            }

            // Build a display URL for the Azure login page.
            const displayUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/authorize`;

            return {
              url: displayUrl,
              instructions:
                "A browser window will open for Azure sign-in. " +
                "Complete the login with your corporate email.",
              method: "auto",
              async callback() {
                try {
                  const tokenCache = await acquireToken(tenantId, clientId);

                  // Save to token-cache.json for engram's Go code.
                  saveTokenCache(tokenCache);
                  log("info", "Authentication successful. Token saved.");

                  const expiresMs = new Date(
                    tokenCache.expires_on,
                  ).getTime();

                  return {
                    type: "success" as const,
                    refresh: "azure-identity-managed",
                    access: tokenCache.access_token,
                    expires: expiresMs,
                  };
                } catch (err) {
                  return {
                    type: "failed" as const,
                  };
                }
              },
            };
          },
        },
      ],
    },
  };

  log("info", "Plugin initialized. Auth provider registered: " + AZURE_ENGRAM_PROVIDER);
  return hooks;
};

// Export as bare default function (legacy format).
// This matches the pattern used by opencode-anthropic-login-via-cli
// which is the most battle-tested plugin format.
//
// Also export as named export for getLegacyPlugins() compatibility,
// which iterates Object.values(mod) looking for functions.
export { plugin as AzureEntraAuthPlugin };
export default plugin;

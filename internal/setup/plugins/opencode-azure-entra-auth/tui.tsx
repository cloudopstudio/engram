/** @jsxImportSource @opentui/solid */
/**
 * TUI plugin for OpenCode — `/engram-login` slash command.
 *
 * Thin adapter that orchestrates dialogs and delegates authentication
 * to the `engram login` Go subprocess (device code flow).
 *
 * @license Apache-2.0
 */

import { readFileSync, existsSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import type { TuiPlugin, TuiPluginModule } from "@opencode-ai/plugin/tui";

// ---- Constants ----

const ENGRAM_DIR = join(homedir(), ".engram");
const CONFIG_PATH = join(ENGRAM_DIR, "config.json");
const TOKEN_CACHE_PATH = join(ENGRAM_DIR, "token-cache.json");
const TOKEN_BUFFER_MS = 5 * 60 * 1000; // 5 min before expiry

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const DEVICE_CODE_RE = /open the page (https:\/\/\S+).*?enter the code (\S+)/i;
const PGSTORE_MISSING_RE = /pgstore build tag/i;
// Matches when the installed engram binary was built WITHOUT the pgstore tag
// and doesn't recognise the `login` command at all.
const LOGIN_UNKNOWN_CMD_RE = /unknown command.*login/i;
const CREDS_MISSING_RE = /requires tenant-id/i;
const IDENTITY_RE = /Authenticated successfully as (\S+)/;

// ---- Types ----

interface EngramConfig {
  "tenant-id"?: string;
  "client-id"?: string;
  "default-profile"?: string;
  profiles?: Record<string, Record<string, string>>;
  [key: string]: unknown;
}

interface TokenCache {
  access_token: string;
  expires_on: string;
}

// ---- Helpers ----

function readEngramConfig(): EngramConfig {
  if (!existsSync(CONFIG_PATH)) return {};
  try {
    return JSON.parse(readFileSync(CONFIG_PATH, "utf-8"));
  } catch {
    return {};
  }
}

function readTokenCache(): TokenCache | null {
  if (!existsSync(TOKEN_CACHE_PATH)) return null;
  try {
    return JSON.parse(readFileSync(TOKEN_CACHE_PATH, "utf-8"));
  } catch {
    return null;
  }
}

function isTokenValid(cache: TokenCache | null): boolean {
  if (!cache?.access_token || !cache.expires_on) return false;
  return Date.now() + TOKEN_BUFFER_MS < new Date(cache.expires_on).getTime();
}

function parseDeviceCode(text: string): { url: string; code: string } | null {
  const m = text.match(DEVICE_CODE_RE);
  return m ? { url: m[1], code: m[2] } : null;
}

/**
 * Resolve the engram binary path, preferring a pgstore-capable build.
 *
 * OpenCode may run with a PATH where the Homebrew engram (built without
 * pgstore) shadows ~/.local/bin/engram. We probe candidate paths in order
 * and return the first one that has the `login` command.
 *
 * Fallback order:
 *   1. ENGRAM_BIN env var (explicit override)
 *   2. ~/.local/bin/engram  (manually installed pgstore build)
 *   3. ~/.opencode/bin/engram
 *   4. "engram" (whatever is on PATH — may or may not have pgstore)
 */
function resolveEngramBin(): string {
  const envOverride = process.env.ENGRAM_BIN;
  if (envOverride) return envOverride;

  const candidates = [
    join(homedir(), ".local", "bin", "engram"),
    join(homedir(), ".opencode", "bin", "engram"),
  ];

  for (const candidate of candidates) {
    if (!existsSync(candidate)) continue;
    try {
      const result = Bun.spawnSync([candidate, "help"], {
        stdout: "pipe",
        stderr: "pipe",
      });
      const output = new TextDecoder().decode(result.stdout) +
        new TextDecoder().decode(result.stderr);
      if (/^\s+login\s/m.test(output)) {
        return candidate;
      }
    } catch {
      // Binary exists but can't run — skip.
    }
  }

  // Fall back to whatever "engram" resolves to on PATH.
  return "engram";
}

function openBrowser(url: string): void {
  const cmd =
    process.platform === "darwin"
      ? ["open", url]
      : process.platform === "win32"
        ? ["cmd", "/c", "start", url]
        : ["xdg-open", url];
  try {
    Bun.spawn(cmd, { stdout: "ignore", stderr: "ignore" });
  } catch {
    // Silent fail — dialog still shows the URL
  }
}

function resolveConfigValue(
  cfg: EngramConfig,
  key: string,
  envVar: string,
  profile?: string,
): string | undefined {
  const env = process.env[envVar];
  if (env) return env;
  const p = profile || cfg["default-profile"];
  if (p && cfg.profiles?.[p]?.[key]) return cfg.profiles[p][key];
  const root = cfg[key];
  return typeof root === "string" && root ? root : undefined;
}

// ---- Dialog helpers ----

function promptDialog(
  api: Parameters<TuiPlugin>[0],
  title: string,
  validate?: (v: string) => string | undefined,
): Promise<string | null> {
  return new Promise((resolve) => {
    api.ui.dialog.replace(
      () => (
        <api.ui.DialogPrompt
          title={title}
          value=""
          onConfirm={(value: string) => {
            if (validate) {
              const err = validate(value);
              if (err) {
                api.ui.toast({
                  variant: "warning",
                  title: "Validation error",
                  message: err,
                  duration: 3000,
                });
                return;
              }
            }
            api.ui.dialog.clear();
            resolve(value.trim());
          }}
          onCancel={() => {
            api.ui.dialog.clear();
            resolve(null);
          }}
        />
      ),
    );
  });
}

// ---- Main login flow ----

async function startLogin(api: Parameters<TuiPlugin>[0]): Promise<void> {
  const cfg = readEngramConfig();

  // --- Profile selection ---
  let profile: string | undefined;
  const profiles = cfg.profiles ? Object.keys(cfg.profiles) : [];
  if (profiles.length > 1) {
    const defaultIdx = profiles.indexOf(cfg["default-profile"] || "");
    const selected = await new Promise<string | null>((resolve) => {
      api.ui.dialog.replace(
        () => (
          <api.ui.DialogSelect
            title="Select engram profile"
            options={profiles.map((p, i) => ({
              title: p === cfg["default-profile"] ? `${p} (default)` : p,
              value: i,
              description: "Azure authentication profile",
            }))}
            current={defaultIdx >= 0 ? defaultIdx : 0}
            onSelect={(item: { title: string; value: number; description?: string }) => {
              api.ui.dialog.clear();
              resolve(profiles[item.value]);
            }}
          />
        ),
      );
    });
    if (!selected) {
      api.ui.toast({
        variant: "info",
        title: "Login cancelled",
        message: "Profile selection dismissed",
        duration: 2000,
      });
      return;
    }
    profile = selected;
  } else if (profiles.length === 1) {
    profile = profiles[0];
  }

  // --- Credential resolution ---
  const envProfile = process.env.ENGRAM_PROFILE || profile;
  let tenantId = resolveConfigValue(cfg, "tenant-id", "AZURE_TENANT_ID", envProfile);
  let clientId = resolveConfigValue(cfg, "client-id", "AZURE_CLIENT_ID", envProfile);

  const validateUUID = (v: string) => (UUID_RE.test(v.trim()) ? undefined : "Must be a valid UUID");

  if (!tenantId) {
    tenantId = await promptDialog(
      api,
      "Azure Tenant ID (UUID format)",
      validateUUID,
    );
    if (!tenantId) {
      api.ui.toast({
        variant: "info",
        title: "Login cancelled",
        message: "Tenant ID not provided",
        duration: 2000,
      });
      return;
    }
    try {
      Bun.spawnSync(["engram", "config", "set", ...(profile ? ["--profile", profile] : []), "tenant-id", tenantId]);
    } catch {
      // Config save failed — continue anyway, creds are in memory
    }
  }

  if (!clientId) {
    clientId = await promptDialog(
      api,
      "Azure Client ID (UUID format)",
      validateUUID,
    );
    if (!clientId) {
      api.ui.toast({
        variant: "info",
        title: "Login cancelled",
        message: "Client ID not provided",
        duration: 2000,
      });
      return;
    }
    try {
      Bun.spawnSync(["engram", "config", "set", ...(profile ? ["--profile", profile] : []), "client-id", clientId]);
    } catch {
      // Config save failed — continue anyway
    }
  }

  // --- Token validity check ---
  const cached = readTokenCache();
  if (isTokenValid(cached)) {
    const expiresAt = new Date(cached!.expires_on).toLocaleTimeString();
    const proceed = await new Promise<boolean>((resolve) => {
      api.ui.dialog.replace(
        () => (
          <api.ui.DialogConfirm
            title="Already authenticated"
            message={`Valid token exists (expires at ${expiresAt}). Re-authenticate anyway?`}
            onConfirm={() => {
              api.ui.dialog.clear();
              resolve(true);
            }}
            onCancel={() => {
              api.ui.dialog.clear();
              resolve(false);
            }}
          />
        ),
      );
    });
    if (!proceed) return;
  }

  // --- Spawn engram login ---
  // Resolve the binary first so we use a pgstore-capable build even when
  // the PATH engram is the standard (non-pgstore) Homebrew release.
  const engramBin = resolveEngramBin();
  console.error(`[engram-login] Using binary: ${engramBin}`);

  let proc: ReturnType<typeof Bun.spawn>;
  try {
    const args = [engramBin, "login"];
    if (profile) args.push("--profile", profile);
    proc = Bun.spawn(args, { stderr: "pipe", stdout: "pipe" });
  } catch {
    api.ui.dialog.replace(
      () => (
        <api.ui.DialogAlert
          title="engram not found"
          message="The engram binary is not installed or not in PATH. Install from: https://github.com/Gentleman-Programming/engram"
          onConfirm={() => {
            api.ui.dialog.clear();
          }}
        />
      ),
    );
    return;
  }

  // Show initial waiting dialog — use raw dialog.replace for custom busy state
  api.ui.dialog.replace(
    () => (
      <box flexDirection="column" paddingLeft={2} paddingRight={2} paddingBottom={1} gap={1}>
        <text>Engram Login</text>
        <text>Starting Azure authentication...</text>
        <box flexDirection="row" gap={1}>
          <text>Connecting to Azure — please wait...</text>
        </box>
        <box flexDirection="row" gap={1}>
          <text>[Press Escape to cancel]</text>
        </box>
      </box>
    ),
  );

  // Read stderr for device code
  let stderrText = "";
  const reader = proc.stderr.getReader();
  const decoder = new TextDecoder();

  let deviceCodeShown = false;

  const readLoop = async () => {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      stderrText += decoder.decode(value, { stream: true });

      // Only show the device code dialog once (message may repeat in chunks).
      if (!deviceCodeShown) {
        const dc = parseDeviceCode(stderrText);
        if (dc) {
          deviceCodeShown = true;
          openBrowser(dc.url);
          // Show device code with custom dialog — no busy/busyText on DialogPrompt
          api.ui.dialog.replace(
            () => (
              <box flexDirection="column" paddingLeft={2} paddingRight={2} paddingBottom={1} gap={1}>
                <text>Engram Login — Device Code</text>
                <text>{`Open: ${dc.url}`}</text>
                <text>{`Enter code: ${dc.code}`}</text>
                <box flexDirection="row" gap={1}>
                  <text>Waiting for Azure sign-in...</text>
                </box>
                <box flexDirection="row" gap={1}>
                  <text>[Browser opened automatically]</text>
                </box>
              </box>
            ),
          );
        }
      }
    }
    // Flush any remaining bytes after the stream closes.
    stderrText += decoder.decode();
  };

  // Wait for both the process exit AND the full stderr drain before
  // evaluating the result. Without this, stderrText may be incomplete
  // when we read it (process exits before readLoop drains the last chunk).
  const [exitCode] = await Promise.all([proc.exited, readLoop().catch(() => {})]);

  if (exitCode === 0) {
    api.ui.dialog.clear();
    const identityMatch = stderrText.match(IDENTITY_RE);
    const msg = identityMatch
      ? `Signed in as ${identityMatch[1]}`
      : "Authentication successful";
    api.ui.toast({
      variant: "success",
      title: "Engram Login",
      message: msg,
      duration: 3000,
    });
  } else {
    if (PGSTORE_MISSING_RE.test(stderrText) || LOGIN_UNKNOWN_CMD_RE.test(stderrText)) {
      // The installed engram binary doesn't have the `login` command — it was
      // built without the pgstore tag (e.g. the standard Homebrew release).
      api.ui.dialog.replace(
        () => (
          <api.ui.DialogAlert
            title="Wrong engram build"
            message={
              "The installed engram binary does not support Azure login.\n\n" +
              "You need the pgstore build. Rebuild from source:\n\n" +
              "  go build -tags pgstore -o ~/.local/bin/engram \\\n" +
              "    github.com/Gentleman-Programming/engram/cmd/engram"
            }
            onConfirm={() => {
              api.ui.dialog.clear();
            }}
          />
        ),
      );
    } else if (CREDS_MISSING_RE.test(stderrText)) {
      api.ui.dialog.replace(
        () => (
          <api.ui.DialogAlert
            title="Credentials missing"
            message="Run /engram-login again to enter your Azure tenant and client IDs."
            onConfirm={() => {
              api.ui.dialog.clear();
            }}
          />
        ),
      );
    } else {
      // Show the last non-empty line of stderr as the error message.
      // This is usually the most specific error from MSAL/azidentity
      // (e.g. "AADSTS70011: The provided request must include a 'scope' input parameter.")
      const lines = stderrText.trim().split("\n").filter((l) => l.trim());
      const errMsg = lines.pop() || "Unknown error";
      console.error(`[engram-login] Authentication failed (exit ${exitCode}): ${errMsg}`);
      api.ui.dialog.replace(
        () => (
          <api.ui.DialogAlert
            title="Authentication failed"
            message={errMsg}
            onConfirm={() => {
              api.ui.dialog.clear();
            }}
          />
        ),
      );
    }
  }
}

// ---- Plugin entry ----

const tui: TuiPlugin = async (api) => {
  api.command.register(() => [
    {
      title: "Engram Login",
      value: "engram.login",
      description: "Authenticate with Azure Entra ID for engram database",
      slash: { name: "engram-login" },
      onSelect: () => startLogin(api),
    },
  ]);
};

export default {
  id: "engram.login",
  tui,
} satisfies TuiPluginModule & { id: string };

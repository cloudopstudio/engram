// Package store implements the persistent memory engine for Engram.
//
// It uses SQLite with FTS5 full-text search to store and retrieve
// observations from AI coding sessions. This is the core of Engram —
// everything else (HTTP server, MCP server, CLI, plugins) talks to this.
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Gentleman-Programming/engram/internal/timeutil"
	sqlite "modernc.org/sqlite"
)

func scanObservationRowSQLite(scanner observationScanner, o *Observation) error {
	return scanner.Scan(
		&o.ID, &o.SyncID, &o.SessionID, &o.Type, &o.Title, &o.Content,
		&o.ToolName, &o.Project, &o.Scope, &o.TopicKey, &o.RevisionCount, &o.DuplicateCount, &o.LastSeenAt, &o.ReviewAfter,
		&o.Pinned, &o.CreatedAt, &o.UpdatedAt, &o.DeletedAt,
	)
}

var openDB = sql.Open

// sqliteConstraintForeignKey is the extended SQLite result code for a foreign-key
// constraint violation (SQLITE_CONSTRAINT_FOREIGNKEY = 787).
// See https://www.sqlite.org/rescode.html#constraint_foreignkey
const sqliteConstraintForeignKey = 787

const (
	sqlitePrimaryBusy   = 5
	sqlitePrimaryLocked = 6
)

var sqliteWriteRetryBackoffs = []time.Duration{
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
}

// Sentinel errors returned by delete operations so callers can use errors.Is.
var (
	ErrSessionDeleteBlocked = errors.New("session deletion is blocked while cloud sync enrollment is active")
	ErrObservationNotFound  = errors.New("observation not found")

	ErrProjectNotFound = errors.New("project not found")
)

// Sentinel errors for relation sync apply path (Phase 2).

// ErrRelationFKMissing is returned by applyRelationUpsertTx when one or
// both observations referenced by the relation payload do not exist locally
// yet. The caller must write the mutation to sync_apply_deferred and ACK
// the sequence so the cursor does not stall.

// ErrCrossProjectRelation is returned by JudgeRelation when the source and
// target observations belong to different projects. The write is rejected
// entirely; no memory_relations row is created and no sync mutation is
// enqueued.

// ErrApplyDead is returned when a deferred relation payload cannot be
// decoded or fails a hard validation. The row is written to
// sync_apply_deferred with apply_status='dead' and is never retried
// automatically; Phase 3 adds a republish CLI.

// ─── Types ───────────────────────────────────────────────────────────────────

const (
	ObservationStateActive      = "active"
	ObservationStateNeedsReview = "needs_review"
)

// State returns the virtual lifecycle state derived from review_after.
func (o Observation) State() string {
	if o.ReviewAfter == nil || strings.TrimSpace(*o.ReviewAfter) == "" {
		return ObservationStateActive
	}
	reviewAfter, err := parseObservationTime(*o.ReviewAfter)
	if err != nil {
		return ObservationStateActive
	}
	if !reviewAfter.After(time.Now().UTC()) {
		return ObservationStateNeedsReview
	}
	return ObservationStateActive
}

// true for the anchor observation

// The anchor observation
// Observations before the focus (chronological)
// Observations after the focus (chronological)
// Session that contains the focus observation

// "all" (default) | "any"

// ─── Decay constants ─────────────────────────────────────────────────────────
//
// Decay defaults — months added to now() to compute review_after on new inserts.
// expires_at is NULL for all types in Phase 1.
const (
	LocalChunkTargetKey = "local"

	// Decay defaults — months added to now() to compute review_after on new inserts.
	// expires_at is NULL for all types in Phase 1.
	decayDecisionMonths   = 6
	decayPolicyMonths     = 12
	decayPreferenceMonths = 3
)

// decayReviewAfterMonths maps observation type → month offset for review_after.
// Types absent from this map get review_after = NULL (Phase 1 behavior).
var decayReviewAfterMonths = map[string]int{
	"decision":   decayDecisionMonths,
	"policy":     decayPolicyMonths,
	"preference": decayPreferenceMonths,
}

const observationSelectColumns = `id, ifnull(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
	       scope, topic_key, revision_count, duplicate_count, last_seen_at, review_after, pinned, created_at, updated_at, deleted_at`

const (
	UpgradeStagePlanned           = "planned"
	UpgradeStageDoctorReady       = "doctor_ready"
	UpgradeStageDoctorBlocked     = "doctor_blocked"
	UpgradeStageRepairApplied     = "repair_applied"
	UpgradeStageBootstrapEnrolled = "bootstrap_enrolled"
	UpgradeStageBootstrapPushed   = "bootstrap_pushed"
	UpgradeStageBootstrapVerified = "bootstrap_verified"
	UpgradeStageRolledBack        = "rolled_back"

	UpgradeRepairClassNone       = "none"
	UpgradeRepairClassReady      = "ready"
	UpgradeRepairClassRepairable = "repairable"
	UpgradeRepairClassBlocked    = "blocked"
	UpgradeRepairClassPolicy     = "policy"
)

type CloudUpgradeSnapshot struct {
	CloudConfigPresent bool   `json:"cloud_config_present"`
	CloudConfigJSON    string `json:"cloud_config_json,omitempty"`
	ProjectEnrolled    bool   `json:"project_enrolled"`
}

type CloudUpgradeState struct {
	Project          string               `json:"project"`
	Stage            string               `json:"stage"`
	RepairClass      string               `json:"repair_class"`
	Snapshot         CloudUpgradeSnapshot `json:"snapshot"`
	LastErrorCode    string               `json:"last_error_code,omitempty"`
	LastErrorMessage string               `json:"last_error_message,omitempty"`
	FindingsJSON     string               `json:"findings_json,omitempty"`
	AppliedActions   string               `json:"applied_actions,omitempty"`
	UpdatedAt        string               `json:"updated_at"`
}

type CloudUpgradeRepairReport struct {
	Class         string `json:"class"`
	ReasonCode    string `json:"reason_code"`
	Message       string `json:"message"`
	PlannedAction string `json:"planned_action,omitempty"`
	Applied       bool   `json:"applied"`
}

type CloudUpgradeLegacyMutationFinding struct {
	Seq        int64  `json:"seq"`
	Entity     string `json:"entity"`
	Op         string `json:"op"`
	ReasonCode string `json:"reason_code"`
	Message    string `json:"message"`
	Repairable bool   `json:"repairable"`
	RepairHint string `json:"repair_hint,omitempty"`
	EntityKey  string `json:"entity_key,omitempty"`
	TargetKey  string `json:"target_key,omitempty"`
	Project    string `json:"project,omitempty"`
}

type CloudUpgradeLegacyMutationReport struct {
	Project         string                              `json:"project"`
	RepairableCount int                                 `json:"repairable_count"`
	BlockedCount    int                                 `json:"blocked_count"`
	Findings        []CloudUpgradeLegacyMutationFinding `json:"findings,omitempty"`
}

// EnrolledProject represents a project enrolled for cloud sync.

// syncRelationPayload is the wire format for a memory_relations row sent over
// the sync_mutations / cloud_mutations rails (entity = 'relation', op = 'upsert').
//
// Phase 2 design §1: 13-field subset of the 17-column memory_relations row.
// Excluded: id (local autoincrement, not portable), superseded_at,
// superseded_by_relation_id (Phase 3 supersede chain).
// omitempty matches the style of syncSessionPayload / syncObservationPayload.

// ExportData is the full serializable dump of the engram database.

// ─── Config ──────────────────────────────────────────────────────────────────

// ─── Types ───────────────────────────────────────────────────────────────────
//
// Public types (Session, Observation, SearchResult, etc.) and the Config
// struct now live in types.go and config.go (no build tags) so they are
// shared between the SQLite and PostgreSQL backends.
//
// Sentinel errors (ErrSessionNotFound, ErrSessionHasObservations,
// ErrPromptNotFound) now live in errors.go.
//
// Sync-related constants (DefaultSyncTargetKey, SyncLifecycle*, SyncEntity*,
// SyncOp*, SyncSource*) now live in sync_constants.go.
//
// Sync payload types (syncSessionPayload, syncObservationPayload,
// syncPromptPayload) and pure-text helpers (passive capture, topic key,
// project normalization, etc.) now live in dedicated files without build
// tags. See passive_capture.go, sync_payload.go, topic_key.go,
// text_helpers.go, project.go, common.go.

// MaxObservationLength returns the configured maximum content length for observations.
func (s *SQLiteStore) MaxObservationLength() int {
	return s.cfg.MaxObservationLength
}

// ─── Store ───────────────────────────────────────────────────────────────────

// SQLiteStore is the SQLite-backed implementation of the Store interface.
type SQLiteStore struct {
	db    *sql.DB
	cfg   Config
	hooks storeHooks
}

type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

type queryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

type sqlRowScanner struct {
	rows *sql.Rows
}

func (r sqlRowScanner) Next() bool {
	return r.rows.Next()
}

func (r sqlRowScanner) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r sqlRowScanner) Err() error {
	return r.rows.Err()
}

func (r sqlRowScanner) Close() error {
	return r.rows.Close()
}

func closeRowsWithError(rows rowScanner, err error) error {
	if closeErr := rows.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

type storeHooks struct {
	exec    func(db execer, query string, args ...any) (sql.Result, error)
	query   func(db queryer, query string, args ...any) (*sql.Rows, error)
	queryIt func(db queryer, query string, args ...any) (rowScanner, error)
	beginTx func(db *sql.DB) (*sql.Tx, error)
	commit  func(tx *sql.Tx) error
}

func defaultStoreHooks() storeHooks {
	return storeHooks{
		exec: func(db execer, query string, args ...any) (sql.Result, error) {
			return db.Exec(query, args...)
		},
		query: func(db queryer, query string, args ...any) (*sql.Rows, error) {
			return db.Query(query, args...)
		},
		queryIt: func(db queryer, query string, args ...any) (rowScanner, error) {
			rows, err := db.Query(query, args...)
			if err != nil {
				return nil, err
			}
			return sqlRowScanner{rows: rows}, nil
		},
		beginTx: func(db *sql.DB) (*sql.Tx, error) {
			return db.Begin()
		},
		commit: func(tx *sql.Tx) error {
			return tx.Commit()
		},
	}
}

// DB returns the underlying *sql.DB. Intended for test helpers and integration
// tests that need to inject raw rows (e.g. legacy data with non-normalized
// project names) without going through the Store's public API.
func (s *SQLiteStore) DB() *sql.DB { return s.db }

func (s *SQLiteStore) execHook(db execer, query string, args ...any) (sql.Result, error) {
	if s.hooks.exec != nil {
		return s.hooks.exec(db, query, args...)
	}
	return db.Exec(query, args...)
}

func (s *SQLiteStore) queryHook(db queryer, query string, args ...any) (*sql.Rows, error) {
	if s.hooks.query != nil {
		return s.hooks.query(db, query, args...)
	}
	return db.Query(query, args...)
}

func (s *SQLiteStore) queryItHook(db queryer, query string, args ...any) (rowScanner, error) {
	if s.hooks.queryIt != nil {
		return s.hooks.queryIt(db, query, args...)
	}
	rows, err := s.queryHook(db, query, args...)
	if err != nil {
		return nil, err
	}
	return sqlRowScanner{rows: rows}, nil
}

func (s *SQLiteStore) beginTxHook() (*sql.Tx, error) {
	if s.hooks.beginTx != nil {
		return s.hooks.beginTx(s.db)
	}
	return s.db.Begin()
}

func (s *SQLiteStore) commitHook(tx *sql.Tx) error {
	if s.hooks.commit != nil {
		return s.hooks.commit(tx)
	}
	return tx.Commit()
}

// NewSQLiteStore opens the SQLite-backed Store. It returns the Store interface
// so callers depend on behavior rather than the concrete *SQLiteStore type.
func NewSQLiteStore(cfg Config) (Store, error) {
	if !filepath.IsAbs(cfg.DataDir) {
		return nil, fmt.Errorf("engram: data directory must be an absolute path, got %q — set ENGRAM_DATA_DIR or ensure your home directory is resolvable", cfg.DataDir)
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("engram: create data dir: %w", err)
	}

	dbPath := filepath.Join(cfg.DataDir, "engram.db")
	db, err := openDB("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("engram: open database: %w", err)
	}
	db.SetMaxOpenConns(1)

	// SQLite performance pragmas
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, fmt.Errorf("engram: pragma %q: %w", p, err)
		}
	}

	s := &SQLiteStore{db: db, cfg: cfg, hooks: defaultStoreHooks()}
	if err := s.migrate(); err != nil {
		return nil, fmt.Errorf("engram: migration: %w", err)
	}
	if err := s.repairEnrolledProjectSyncMutations(); err != nil {
		return nil, fmt.Errorf("engram: repair enrolled sync journal: %w", err)
	}

	return s, nil
}

// newWithoutRepair is the same as New but skips repairEnrolledProjectSyncMutations.
// It exists solely to support tests that need to seed data and call repair manually.
func newWithoutRepair(cfg Config) (*SQLiteStore, error) {
	if !filepath.IsAbs(cfg.DataDir) {
		return nil, fmt.Errorf("engram: data directory must be an absolute path, got %q — set ENGRAM_DATA_DIR or ensure your home directory is resolvable", cfg.DataDir)
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("engram: create data dir: %w", err)
	}

	dbPath := filepath.Join(cfg.DataDir, "engram.db")
	db, err := openDB("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("engram: open database: %w", err)
	}
	db.SetMaxOpenConns(1)

	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, fmt.Errorf("engram: pragma %q: %w", p, err)
		}
	}

	s := &SQLiteStore{db: db, cfg: cfg, hooks: defaultStoreHooks()}
	if err := s.migrate(); err != nil {
		return nil, fmt.Errorf("engram: migration: %w", err)
	}
	return s, nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// newWithoutRepairSQLite creates a SQLiteStore that runs migrations but
// skips the startup repair pass. Used by tests that need to seed raw data
// before calling repairEnrolledProjectSyncMutations explicitly.
func newWithoutRepairSQLite(cfg Config) (*SQLiteStore, error) {
	if !filepath.IsAbs(cfg.DataDir) {
		return nil, fmt.Errorf("engram: data directory must be an absolute path, got %q", cfg.DataDir)
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("engram: create data dir: %w", err)
	}
	dbPath := filepath.Join(cfg.DataDir, "engram.db")
	db, err := openDB("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("engram: open database: %w", err)
	}
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, fmt.Errorf("engram: pragma %q: %w", p, err)
		}
	}
	s := &SQLiteStore{db: db, cfg: cfg, hooks: defaultStoreHooks()}
	if err := s.migrate(); err != nil {
		return nil, fmt.Errorf("engram: migration: %w", err)
	}
	return s, nil
}

// Identity returns the identity associated with the store.
// For the SQLite backend, this is empty unless explicitly set.
func (s *SQLiteStore) Identity() string {
	return ""
}

// ─── Migrations ──────────────────────────────────────────────────────────────

func (s *SQLiteStore) migrate() error {
	schema := `
			CREATE TABLE IF NOT EXISTS sessions (
				id         TEXT PRIMARY KEY,
			project    TEXT NOT NULL,
			directory  TEXT NOT NULL,
			started_at TEXT NOT NULL DEFAULT (datetime('now')),
			ended_at   TEXT,
			summary    TEXT
		);

			CREATE TABLE IF NOT EXISTS observations (
				id         INTEGER PRIMARY KEY AUTOINCREMENT,
				sync_id    TEXT,
				session_id TEXT    NOT NULL,
			type       TEXT    NOT NULL,
			title      TEXT    NOT NULL,
			content    TEXT    NOT NULL,
			tool_name  TEXT,
			project    TEXT,
			scope      TEXT    NOT NULL DEFAULT 'project',
			topic_key  TEXT,
			normalized_hash TEXT,
			revision_count INTEGER NOT NULL DEFAULT 1,
			duplicate_count INTEGER NOT NULL DEFAULT 1,
			last_seen_at TEXT,
			pinned     BOOLEAN NOT NULL DEFAULT 0,
			created_at TEXT    NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT    NOT NULL DEFAULT (datetime('now')),
			deleted_at TEXT,
			created_by TEXT,
			FOREIGN KEY (session_id) REFERENCES sessions(id)
		);

		CREATE INDEX IF NOT EXISTS idx_obs_session  ON observations(session_id);
		CREATE INDEX IF NOT EXISTS idx_obs_type     ON observations(type);
		CREATE INDEX IF NOT EXISTS idx_obs_project  ON observations(project);
		CREATE INDEX IF NOT EXISTS idx_obs_created  ON observations(created_at DESC);

		CREATE VIRTUAL TABLE IF NOT EXISTS observations_fts USING fts5(
			title,
			content,
			tool_name,
			type,
			project,
			topic_key,
			content='observations',
			content_rowid='id'
		);

			CREATE TABLE IF NOT EXISTS user_prompts (
				id         INTEGER PRIMARY KEY AUTOINCREMENT,
				sync_id    TEXT,
				session_id TEXT    NOT NULL,
			content    TEXT    NOT NULL,
			project    TEXT,
			created_at TEXT    NOT NULL DEFAULT (datetime('now')),
			FOREIGN KEY (session_id) REFERENCES sessions(id)
		);

			CREATE TABLE IF NOT EXISTS prompt_tombstones (
				sync_id    TEXT PRIMARY KEY,
				session_id TEXT,
				project    TEXT,
				deleted_at TEXT NOT NULL DEFAULT (datetime('now'))
			);

		CREATE INDEX IF NOT EXISTS idx_prompts_session ON user_prompts(session_id);
		CREATE INDEX IF NOT EXISTS idx_prompts_project ON user_prompts(project);
		CREATE INDEX IF NOT EXISTS idx_prompts_created ON user_prompts(created_at DESC);

		CREATE VIRTUAL TABLE IF NOT EXISTS prompts_fts USING fts5(
			content,
			project,
			content='user_prompts',
			content_rowid='id'
		);

			CREATE TABLE IF NOT EXISTS sync_chunks (
				target_key  TEXT NOT NULL DEFAULT 'local',
				chunk_id    TEXT NOT NULL,
				imported_at TEXT NOT NULL DEFAULT (datetime('now')),
				PRIMARY KEY (target_key, chunk_id)
			);

			CREATE TABLE IF NOT EXISTS sync_state (
				target_key           TEXT PRIMARY KEY,
				lifecycle            TEXT NOT NULL DEFAULT 'idle',
				last_enqueued_seq    INTEGER NOT NULL DEFAULT 0,
				last_acked_seq       INTEGER NOT NULL DEFAULT 0,
				last_pulled_seq      INTEGER NOT NULL DEFAULT 0,
				consecutive_failures INTEGER NOT NULL DEFAULT 0,
				backoff_until        TEXT,
				lease_owner          TEXT,
				lease_until          TEXT,
				last_error           TEXT,
				updated_at           TEXT NOT NULL DEFAULT (datetime('now'))
			);

			CREATE TABLE IF NOT EXISTS sync_mutations (
				seq         INTEGER PRIMARY KEY AUTOINCREMENT,
				target_key  TEXT NOT NULL,
				entity      TEXT NOT NULL,
				entity_key  TEXT NOT NULL,
				op          TEXT NOT NULL,
				payload     TEXT NOT NULL,
				source      TEXT NOT NULL DEFAULT 'local',
				occurred_at TEXT NOT NULL DEFAULT (datetime('now')),
				acked_at    TEXT,
				FOREIGN KEY (target_key) REFERENCES sync_state(target_key)
			);

			CREATE TABLE IF NOT EXISTS cloud_upgrade_state (
				project            TEXT PRIMARY KEY,
				stage              TEXT NOT NULL DEFAULT 'planned',
				repair_class       TEXT NOT NULL DEFAULT 'none',
				snapshot_json      TEXT NOT NULL DEFAULT '{}',
				last_error_code    TEXT,
				last_error_message TEXT,
				findings_json      TEXT,
				applied_actions    TEXT,
				updated_at         TEXT NOT NULL DEFAULT (datetime('now'))
			);
		`
	if _, err := s.execHook(s.db, schema); err != nil {
		return err
	}

	observationColumns := []struct {
		name       string
		definition string
	}{
		{name: "sync_id", definition: "TEXT"},
		{name: "scope", definition: "TEXT NOT NULL DEFAULT 'project'"},
		{name: "topic_key", definition: "TEXT"},
		{name: "normalized_hash", definition: "TEXT"},
		{name: "revision_count", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "duplicate_count", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "last_seen_at", definition: "TEXT"},
		{name: "pinned", definition: "BOOLEAN NOT NULL DEFAULT 0"},
		{name: "updated_at", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "deleted_at", definition: "TEXT"},
		{name: "created_by", definition: "TEXT"},
	}
	for _, c := range observationColumns {
		if err := s.addColumnIfNotExists("observations", c.name, c.definition); err != nil {
			return err
		}
	}

	if err := s.migrateLegacyObservationsTable(); err != nil {
		return err
	}

	if err := s.addColumnIfNotExists("user_prompts", "sync_id", "TEXT"); err != nil {
		return err
	}

	if _, err := s.execHook(s.db, `
		CREATE INDEX IF NOT EXISTS idx_obs_scope ON observations(scope);
		CREATE INDEX IF NOT EXISTS idx_obs_sync_id ON observations(sync_id);
		CREATE INDEX IF NOT EXISTS idx_obs_topic ON observations(topic_key, project, scope, updated_at DESC);
		CREATE INDEX IF NOT EXISTS idx_obs_deleted ON observations(deleted_at);
		CREATE INDEX IF NOT EXISTS idx_obs_dedupe ON observations(normalized_hash, project, scope, type, title, created_at DESC);
		CREATE INDEX IF NOT EXISTS idx_prompts_sync_id ON user_prompts(sync_id);
		CREATE INDEX IF NOT EXISTS idx_prompt_tombstones_project ON prompt_tombstones(project, deleted_at DESC);
		CREATE INDEX IF NOT EXISTS idx_sync_mutations_target_seq ON sync_mutations(target_key, seq);
		CREATE INDEX IF NOT EXISTS idx_sync_mutations_pending ON sync_mutations(target_key, acked_at, seq);
	`); err != nil {
		return err
	}

	// Project-scoped sync: add project column to sync_mutations and enrollment table.
	if err := s.addColumnIfNotExists("sync_mutations", "project", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := s.addColumnIfNotExists("sync_state", "reason_code", "TEXT"); err != nil {
		return err
	}
	if err := s.addColumnIfNotExists("sync_state", "reason_message", "TEXT"); err != nil {
		return err
	}
	if err := s.migrateSyncChunksTable(); err != nil {
		return err
	}

	// ── Phase: memory-conflict-surfacing — B.1 ──────────────────────────────
	// Additive nullable columns on observations for conflict surfacing, decay,
	// and embedding reservation.  All applied via addColumnIfNotExists so that
	// running migrate() on a fresh DB (where CREATE TABLE already added these
	// columns) is a no-op.
	memConflictObsCols := []struct {
		name       string
		definition string
	}{
		{name: "review_after", definition: "TEXT"},
		{name: "expires_at", definition: "TEXT"},
		{name: "embedding", definition: "BLOB"},
		{name: "embedding_model", definition: "TEXT"},
		{name: "embedding_created_at", definition: "TEXT"},
	}
	for _, c := range memConflictObsCols {
		if err := s.addColumnIfNotExists("observations", c.name, c.definition); err != nil {
			return err
		}
	}

	// ── Phase: memory-conflict-surfacing — B.2 ──────────────────────────────
	// Create the memory_relations table (idempotent via IF NOT EXISTS).
	// source_id / target_id are TEXT sync_id keys (cross-machine portable).
	// NO UNIQUE on (source_id, target_id) — multi-actor disagreement allowed.
	if _, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS memory_relations (
			id                        INTEGER PRIMARY KEY AUTOINCREMENT,
			sync_id                   TEXT    NOT NULL UNIQUE,
			source_id                 TEXT,
			target_id                 TEXT,
			relation                  TEXT    NOT NULL DEFAULT 'pending',
			reason                    TEXT,
			evidence                  TEXT,
			confidence                REAL,
			judgment_status           TEXT    NOT NULL DEFAULT 'pending',
			marked_by_actor           TEXT,
			marked_by_kind            TEXT,
			marked_by_model           TEXT,
			session_id                TEXT,
			superseded_at             TEXT,
			superseded_by_relation_id INTEGER REFERENCES memory_relations(id) ON DELETE SET NULL,
			created_at                TEXT    NOT NULL DEFAULT (datetime('now')),
			updated_at                TEXT    NOT NULL DEFAULT (datetime('now'))
		);
	`); err != nil {
		return err
	}

	// ── Phase: memory-conflict-surfacing — B.3 ──────────────────────────────
	// Indexes for memory_relations (all idempotent via IF NOT EXISTS).
	if _, err := s.execHook(s.db, `
		CREATE INDEX IF NOT EXISTS idx_memrel_source    ON memory_relations(source_id, judgment_status);
		CREATE INDEX IF NOT EXISTS idx_memrel_target    ON memory_relations(target_id, judgment_status);
		CREATE INDEX IF NOT EXISTS idx_memrel_supersede ON memory_relations(superseded_by_relation_id);
	`); err != nil {
		return err
	}

	if _, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS sync_enrolled_projects (
			project     TEXT PRIMARY KEY,
			enrolled_at TEXT NOT NULL DEFAULT (datetime('now'))
		);
		CREATE INDEX IF NOT EXISTS idx_sync_mutations_project ON sync_mutations(project);
	`); err != nil {
		return err
	}

	// project_metadata table for project deprecation.
	if _, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS project_metadata (
			project       TEXT PRIMARY KEY,
			deprecated    INTEGER NOT NULL DEFAULT 0,
			deprecated_at TEXT,
			deprecated_by TEXT,
			description   TEXT,
			created_at    TEXT NOT NULL DEFAULT (datetime('now')),
			updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
		);
	`); err != nil {
		return err
	}
	// Backfill: extract project from JSON payload for existing rows with empty project.
	if _, err := s.execHook(s.db, `
		UPDATE sync_mutations
		SET project = COALESCE(json_extract(payload, '$.project'), '')
		WHERE project = '' AND payload != ''
	`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `
		UPDATE sync_mutations
		SET project = COALESCE((
			SELECT sessions.project
			FROM sessions
			WHERE sessions.id = json_extract(sync_mutations.payload, '$.session_id')
		), '')
		WHERE project = ''
		  AND payload != ''
		  AND ifnull(json_extract(payload, '$.session_id'), '') != ''
	`); err != nil {
		return err
	}

	if _, err := s.execHook(s.db, `UPDATE observations SET scope = 'project' WHERE scope IS NULL OR scope = ''`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `UPDATE observations SET topic_key = NULL WHERE topic_key = ''`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `UPDATE observations SET revision_count = 1 WHERE revision_count IS NULL OR revision_count < 1`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `UPDATE observations SET duplicate_count = 1 WHERE duplicate_count IS NULL OR duplicate_count < 1`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `UPDATE observations SET updated_at = created_at WHERE updated_at IS NULL OR updated_at = ''`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `UPDATE observations SET sync_id = 'obs-' || lower(hex(randomblob(16))) WHERE sync_id IS NULL OR sync_id = ''`); err != nil {
		return err
	}

	if _, err := s.execHook(s.db, `UPDATE user_prompts SET project = '' WHERE project IS NULL`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `UPDATE prompt_tombstones SET project = '' WHERE project IS NULL`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `UPDATE user_prompts SET sync_id = 'prompt-' || lower(hex(randomblob(16))) WHERE sync_id IS NULL OR sync_id = ''`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `INSERT OR IGNORE INTO sync_state (target_key, lifecycle, updated_at) VALUES ('cloud', 'idle', datetime('now'))`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `
		CREATE INDEX IF NOT EXISTS idx_cloud_upgrade_state_stage ON cloud_upgrade_state(stage);
		CREATE INDEX IF NOT EXISTS idx_sync_mutations_lookup ON sync_mutations(target_key, entity, entity_key, source);
	`); err != nil {
		return err
	}

	// Create triggers to keep FTS in sync (idempotent check)
	var name string
	err := s.db.QueryRow(
		"SELECT name FROM sqlite_master WHERE type='trigger' AND name='obs_fts_insert'",
	).Scan(&name)

	if err == sql.ErrNoRows {
		triggers := `
			CREATE TRIGGER obs_fts_insert AFTER INSERT ON observations BEGIN
				INSERT INTO observations_fts(rowid, title, content, tool_name, type, project, topic_key)
				VALUES (new.id, new.title, new.content, new.tool_name, new.type, new.project, new.topic_key);
			END;

			CREATE TRIGGER obs_fts_delete AFTER DELETE ON observations BEGIN
				INSERT INTO observations_fts(observations_fts, rowid, title, content, tool_name, type, project, topic_key)
				VALUES ('delete', old.id, old.title, old.content, old.tool_name, old.type, old.project, old.topic_key);
			END;

			CREATE TRIGGER obs_fts_update AFTER UPDATE ON observations BEGIN
				INSERT INTO observations_fts(observations_fts, rowid, title, content, tool_name, type, project, topic_key)
				VALUES ('delete', old.id, old.title, old.content, old.tool_name, old.type, old.project, old.topic_key);
				INSERT INTO observations_fts(rowid, title, content, tool_name, type, project, topic_key)
				VALUES (new.id, new.title, new.content, new.tool_name, new.type, new.project, new.topic_key);
			END;
		`
		if _, err := s.execHook(s.db, triggers); err != nil {
			return err
		}
	}

	if err := s.migrateFTSTopicKey(); err != nil {
		return err
	}

	// ── Phase: memory-conflict-surfacing — B.2 ──────────────────────────────
	// Create the memory_relations table (idempotent via IF NOT EXISTS).
	// source_id / target_id are TEXT sync_id keys (cross-machine portable).
	// NO UNIQUE on (source_id, target_id) — multi-actor disagreement allowed.
	if _, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS memory_relations (
			id                        INTEGER PRIMARY KEY AUTOINCREMENT,
			sync_id                   TEXT    NOT NULL UNIQUE,
			source_id                 TEXT,
			target_id                 TEXT,
			relation                  TEXT    NOT NULL DEFAULT 'pending',
			reason                    TEXT,
			evidence                  TEXT,
			confidence                REAL,
			judgment_status           TEXT    NOT NULL DEFAULT 'pending',
			marked_by_actor           TEXT,
			marked_by_kind            TEXT,
			marked_by_model           TEXT,
			session_id                TEXT,
			superseded_at             TEXT,
			superseded_by_relation_id INTEGER REFERENCES memory_relations(id) ON DELETE SET NULL,
			created_at                TEXT    NOT NULL DEFAULT (datetime('now')),
			updated_at                TEXT    NOT NULL DEFAULT (datetime('now'))
		);
	`); err != nil {
		return err
	}

	// ── Phase: memory-conflict-surfacing — B.3 ──────────────────────────────
	// Indexes for memory_relations (all idempotent via IF NOT EXISTS).
	if _, err := s.execHook(s.db, `
		CREATE INDEX IF NOT EXISTS idx_memrel_source    ON memory_relations(source_id, judgment_status);
		CREATE INDEX IF NOT EXISTS idx_memrel_target    ON memory_relations(target_id, judgment_status);
		CREATE INDEX IF NOT EXISTS idx_memrel_supersede ON memory_relations(superseded_by_relation_id);
	`); err != nil {
		return err
	}

	// ── Phase: memory-conflict-surfacing — deferred queue ───────────────────
	// sync_apply_deferred holds relation mutations that could not be applied
	// immediately (e.g. FK misses). Required by GetRelationStats (deferred/dead counts).
	if _, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS sync_apply_deferred (
			sync_id           TEXT    PRIMARY KEY,
			entity            TEXT    NOT NULL,
			payload           TEXT    NOT NULL,
			apply_status      TEXT    NOT NULL DEFAULT 'deferred',
			retry_count       INTEGER NOT NULL DEFAULT 0,
			last_error        TEXT,
			last_attempted_at TEXT,
			first_seen_at     TEXT    NOT NULL DEFAULT (datetime('now'))
		);
		CREATE INDEX IF NOT EXISTS idx_sad_status_seen
			ON sync_apply_deferred(apply_status, first_seen_at);
	`); err != nil {
		return err
	}

	// Phase 3b: composite index for efficient conflict-audit list/count queries.
	if _, err := s.execHook(s.db, `
		CREATE INDEX IF NOT EXISTS idx_memrel_status_created
			ON memory_relations(judgment_status, created_at DESC);
	`); err != nil {
		return err
	}

	// Phase decay-v1: additive nullable columns for decay scheduling.
	// review_after holds the ISO-8601 timestamp after which this observation
	// should be reviewed/refreshed. expires_at is reserved for Phase 2 (NULL now).
	for _, c := range []struct{ name, def string }{
		{"review_after", "TEXT"},
		{"expires_at", "TEXT"},
	} {
		if err := s.addColumnIfNotExists("observations", c.name, c.def); err != nil {
			return err
		}
	}

	// Prompts FTS triggers (separate idempotent check)
	var promptTrigger string
	err = s.db.QueryRow(
		"SELECT name FROM sqlite_master WHERE type='trigger' AND name='prompt_fts_insert'",
	).Scan(&promptTrigger)

	if err == sql.ErrNoRows {
		promptTriggers := `
			CREATE TRIGGER prompt_fts_insert AFTER INSERT ON user_prompts BEGIN
				INSERT INTO prompts_fts(rowid, content, project)
				VALUES (new.id, new.content, new.project);
			END;

			CREATE TRIGGER prompt_fts_delete AFTER DELETE ON user_prompts BEGIN
				INSERT INTO prompts_fts(prompts_fts, rowid, content, project)
				VALUES ('delete', old.id, old.content, old.project);
			END;

			CREATE TRIGGER prompt_fts_update AFTER UPDATE ON user_prompts BEGIN
				INSERT INTO prompts_fts(prompts_fts, rowid, content, project)
				VALUES ('delete', old.id, old.content, old.project);
				INSERT INTO prompts_fts(rowid, content, project)
				VALUES (new.id, new.content, new.project);
			END;
		`
		if _, err := s.execHook(s.db, promptTriggers); err != nil {
			return err
		}
	}

	// Phase 3: add republish CLI, surface dead rows via mem_status.
	if _, err := s.execHook(s.db, `
		CREATE TABLE IF NOT EXISTS sync_apply_deferred (
			sync_id           TEXT    PRIMARY KEY,
			entity            TEXT    NOT NULL,
			payload           TEXT    NOT NULL,
			apply_status      TEXT    NOT NULL DEFAULT 'deferred',
			retry_count       INTEGER NOT NULL DEFAULT 0,
			last_error        TEXT,
			last_attempted_at TEXT,
			first_seen_at     TEXT    NOT NULL DEFAULT (datetime('now'))
		);
		CREATE INDEX IF NOT EXISTS idx_sad_status_seen
			ON sync_apply_deferred(apply_status, first_seen_at);
	`); err != nil {
		return err
	}

	// Phase 3b: composite index for conflict-audit list/count queries.
	if _, err := s.execHook(s.db, `
		CREATE INDEX IF NOT EXISTS idx_memrel_status_created
			ON memory_relations(judgment_status, created_at DESC);
	`); err != nil {
		return err
	}

	// Autosync Phase: add reason_code and reason_message columns to sync_state.
	// These are used by MarkSyncBlocked and ApplyPulledMutation to surface
	// deterministic block reasons (auth_required, non_enrolled_pending_mutations, etc.).
	for _, c := range []struct{ col, def string }{
		{"reason_code", "TEXT"},
		{"reason_message", "TEXT"},
	} {
		if err := s.addColumnIfNotExists("sync_state", c.col, c.def); err != nil {
			return err
		}
	}

	return nil
}

func (s *SQLiteStore) SaveCloudUpgradeState(state CloudUpgradeState) error {
	project, _ := NormalizeProject(state.Project)
	project = strings.TrimSpace(project)
	if project == "" {
		return fmt.Errorf("cloud upgrade project must not be empty")
	}
	state.Project = project
	state.Stage = normalizeUpgradeStage(state.Stage)
	state.RepairClass = normalizeUpgradeRepairClass(state.RepairClass)

	snapshotJSON, err := json.Marshal(state.Snapshot)
	if err != nil {
		return fmt.Errorf("marshal cloud upgrade snapshot: %w", err)
	}

	_, err = s.execHook(s.db, `
		INSERT INTO cloud_upgrade_state (
			project, stage, repair_class, snapshot_json, last_error_code, last_error_message, findings_json, applied_actions, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
		ON CONFLICT(project) DO UPDATE SET
			stage = excluded.stage,
			repair_class = excluded.repair_class,
			snapshot_json = excluded.snapshot_json,
			last_error_code = excluded.last_error_code,
			last_error_message = excluded.last_error_message,
			findings_json = excluded.findings_json,
			applied_actions = excluded.applied_actions,
			updated_at = datetime('now')
	`, state.Project, state.Stage, state.RepairClass, string(snapshotJSON), nullableString(state.LastErrorCode), nullableString(state.LastErrorMessage), nullableString(state.FindingsJSON), nullableString(state.AppliedActions))
	return err
}

func (s *SQLiteStore) GetCloudUpgradeState(project string) (*CloudUpgradeState, error) {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, nil
	}

	row := s.db.QueryRow(`
		SELECT project, stage, repair_class, snapshot_json, ifnull(last_error_code, ''), ifnull(last_error_message, ''), ifnull(findings_json, ''), ifnull(applied_actions, ''), updated_at
		FROM cloud_upgrade_state
		WHERE project = ?
	`, project)

	var state CloudUpgradeState
	var snapshotJSON string
	if err := row.Scan(&state.Project, &state.Stage, &state.RepairClass, &snapshotJSON, &state.LastErrorCode, &state.LastErrorMessage, &state.FindingsJSON, &state.AppliedActions, &state.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if strings.TrimSpace(snapshotJSON) != "" {
		if err := json.Unmarshal([]byte(snapshotJSON), &state.Snapshot); err != nil {
			return nil, fmt.Errorf("parse cloud upgrade snapshot: %w", err)
		}
	}
	state.Stage = normalizeUpgradeStage(state.Stage)
	state.RepairClass = normalizeUpgradeRepairClass(state.RepairClass)
	return &state, nil
}

func (s *SQLiteStore) ClearCloudUpgradeState(project string) error {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil
	}
	_, err := s.execHook(s.db, `DELETE FROM cloud_upgrade_state WHERE project = ?`, project)
	return err
}

func (s *SQLiteStore) CanRollbackCloudUpgrade(project string) (bool, error) {
	state, err := s.GetCloudUpgradeState(project)
	if err != nil {
		return false, err
	}
	if state == nil {
		return false, nil
	}
	return state.Stage != UpgradeStageBootstrapVerified, nil
}

func (s *SQLiteStore) RollbackCloudUpgrade(project string) (CloudUpgradeState, error) {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return CloudUpgradeState{}, fmt.Errorf("cloud upgrade rollback requires project")
	}

	state, err := s.GetCloudUpgradeState(project)
	if err != nil {
		return CloudUpgradeState{}, fmt.Errorf("read cloud upgrade rollback state: %w", err)
	}
	if state == nil {
		return CloudUpgradeState{}, fmt.Errorf("rollback requires existing upgrade checkpoint state")
	}
	if state.Stage == UpgradeStageBootstrapVerified {
		return CloudUpgradeState{}, fmt.Errorf("rollback is unavailable post-bootstrap; use explicit disconnect/unenroll flows")
	}

	if state.Snapshot.ProjectEnrolled {
		if err := s.EnrollProject(project); err != nil {
			return CloudUpgradeState{}, fmt.Errorf("restore project enrollment from rollback snapshot: %w", err)
		}
	} else {
		if err := s.UnenrollProject(project); err != nil {
			return CloudUpgradeState{}, fmt.Errorf("restore project unenrollment from rollback snapshot: %w", err)
		}
	}

	state.Stage = UpgradeStageRolledBack
	state.LastErrorCode = ""
	state.LastErrorMessage = ""
	state.FindingsJSON = ""
	state.AppliedActions = ""
	if err := s.SaveCloudUpgradeState(*state); err != nil {
		return CloudUpgradeState{}, fmt.Errorf("persist rolled back upgrade state: %w", err)
	}

	rolledBack, err := s.GetCloudUpgradeState(project)
	if err != nil {
		return CloudUpgradeState{}, fmt.Errorf("load rolled back cloud upgrade state: %w", err)
	}
	if rolledBack == nil {
		return CloudUpgradeState{}, fmt.Errorf("rolled back cloud upgrade state not found")
	}
	return *rolledBack, nil
}

func (s *SQLiteStore) RepairCloudUpgrade(project string, apply bool) (CloudUpgradeRepairReport, error) {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return CloudUpgradeRepairReport{
			Class:      UpgradeRepairClassBlocked,
			ReasonCode: "upgrade_blocked_project_required",
			Message:    "project is required for cloud upgrade repair",
		}, nil
	}

	if blocked, report, err := s.cloudUpgradeManualActionReport(project); err != nil {
		return CloudUpgradeRepairReport{}, err
	} else if blocked {
		return report, nil
	}

	enrolled, err := s.IsProjectEnrolled(project)
	if err != nil {
		return CloudUpgradeRepairReport{}, fmt.Errorf("check project enrollment: %w", err)
	}
	if !enrolled {
		return CloudUpgradeRepairReport{
			Class:      UpgradeRepairClassBlocked,
			ReasonCode: "upgrade_blocked_manual",
			Message:    fmt.Sprintf("project %q is not enrolled; run doctor/bootstrap guidance first", project),
		}, nil
	}

	legacyReport, err := s.DiagnoseCloudUpgradeLegacyMutations(project)
	if err != nil {
		return CloudUpgradeRepairReport{}, fmt.Errorf("diagnose legacy cloud upgrade mutations: %w", err)
	}
	// When both repairable and blocked mutations coexist we must not hold the
	// entire repair pass hostage to the non-repairable entries. Apply the
	// repairable subset first, then surface the residual blockers.
	appliedRepairs := false
	if apply && legacyReport.RepairableCount > 0 {
		if err := s.applyCloudUpgradeLegacyMutationRepairs(project); err != nil {
			return CloudUpgradeRepairReport{}, fmt.Errorf("apply cloud upgrade legacy mutation repairs: %w", err)
		}
		appliedRepairs = true
	}

	if legacyReport.BlockedCount > 0 {
		// Scan for the first non-repairable finding so the operator sees the
		// actual blocker. Findings[0] is ordered by seq and may be a repairable
		// entry, which previously produced a misleading error message (#446).
		var blocked CloudUpgradeLegacyMutationFinding
		for _, f := range legacyReport.Findings {
			if !f.Repairable {
				blocked = f
				break
			}
		}
		var msg string
		switch {
		case appliedRepairs:
			msg = fmt.Sprintf("applied %d repairable payload(s); %d remain blocked: manual-action-required: %s (seq=%d entity=%s entity_key=%q op=%s)",
				legacyReport.RepairableCount, legacyReport.BlockedCount, blocked.Message, blocked.Seq, blocked.Entity, blocked.EntityKey, blocked.Op)
		case legacyReport.RepairableCount > 0:
			msg = fmt.Sprintf("%d repairable payload(s) would apply; %d would remain blocked: manual-action-required: %s (seq=%d entity=%s entity_key=%q op=%s)",
				legacyReport.RepairableCount, legacyReport.BlockedCount, blocked.Message, blocked.Seq, blocked.Entity, blocked.EntityKey, blocked.Op)
		default:
			msg = fmt.Sprintf("manual-action-required: %s (seq=%d entity=%s entity_key=%q op=%s)",
				blocked.Message, blocked.Seq, blocked.Entity, blocked.EntityKey, blocked.Op)
		}
		return CloudUpgradeRepairReport{
			Class:      UpgradeRepairClassBlocked,
			ReasonCode: UpgradeReasonBlockedLegacyMutationManual,
			Message:    msg,
			Applied:    appliedRepairs,
		}, nil
	}

	if legacyReport.RepairableCount > 0 {
		if !appliedRepairs {
			return CloudUpgradeRepairReport{
				Class:         UpgradeRepairClassRepairable,
				ReasonCode:    UpgradeReasonRepairableLegacyMutationPayload,
				Message:       fmt.Sprintf("project %q has %d repairable legacy mutation payload issue(s)", project, legacyReport.RepairableCount),
				PlannedAction: "repair_legacy_mutation_payloads",
				Applied:       false,
			}, nil
		}
		_ = s.SaveCloudUpgradeState(CloudUpgradeState{
			Project:     project,
			Stage:       UpgradeStageRepairApplied,
			RepairClass: UpgradeRepairClassRepairable,
		})
		return CloudUpgradeRepairReport{
			Class:         UpgradeRepairClassRepairable,
			ReasonCode:    UpgradeReasonRepairableLegacyMutationPayload,
			Message:       fmt.Sprintf("applied deterministic legacy mutation payload repairs for project %q", project),
			PlannedAction: "repair_legacy_mutation_payloads",
			Applied:       true,
		}, nil
	}

	requiresBackfill, err := s.projectSyncBackfillRequired(project)
	if err != nil {
		return CloudUpgradeRepairReport{}, err
	}
	if !requiresBackfill {
		return CloudUpgradeRepairReport{
			Class:      UpgradeRepairClassReady,
			ReasonCode: "upgrade_repair_noop",
			Message:    fmt.Sprintf("project %q has no deterministic local repairs to apply", project),
		}, nil
	}

	report := CloudUpgradeRepairReport{
		Class:         UpgradeRepairClassRepairable,
		ReasonCode:    "upgrade_repair_backfill_sync_journal",
		Message:       fmt.Sprintf("project %q has deterministic local sync metadata gaps", project),
		PlannedAction: "backfill_sync_journal",
		Applied:       false,
	}
	if !apply {
		return report, nil
	}

	if err := s.withTx(func(tx *sql.Tx) error {
		return s.backfillProjectSyncMutationsTx(tx, project)
	}); err != nil {
		return CloudUpgradeRepairReport{}, fmt.Errorf("apply cloud upgrade repair: %w", err)
	}
	report.Applied = true
	_ = s.SaveCloudUpgradeState(CloudUpgradeState{
		Project:     project,
		Stage:       UpgradeStageRepairApplied,
		RepairClass: UpgradeRepairClassRepairable,
	})
	return report, nil
}

type cloudUpgradeLegacyMutationEvaluation struct {
	finding         CloudUpgradeLegacyMutationFinding
	hasIssue        bool
	repairedPayload string
	canRepair       bool
}

func (s *SQLiteStore) DiagnoseCloudUpgradeLegacyMutations(project string) (CloudUpgradeLegacyMutationReport, error) {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return CloudUpgradeLegacyMutationReport{Project: project}, nil
	}

	evaluations, err := s.evaluateCloudUpgradeLegacyMutations(project)
	if err != nil {
		return CloudUpgradeLegacyMutationReport{}, err
	}
	report := CloudUpgradeLegacyMutationReport{Project: project}
	for _, eval := range evaluations {
		if !eval.hasIssue {
			continue
		}
		report.Findings = append(report.Findings, eval.finding)
		if eval.canRepair {
			report.RepairableCount++
		} else {
			report.BlockedCount++
		}
	}
	return report, nil
}

func (s *SQLiteStore) applyCloudUpgradeLegacyMutationRepairs(project string) error {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil
	}
	return s.withTx(func(tx *sql.Tx) error {
		mutations, err := s.listPendingProjectMutationsTx(tx, project)
		if err != nil {
			return err
		}
		for _, mutation := range mutations {
			eval, err := s.evaluateCloudUpgradeLegacyMutationTx(tx, mutation)
			if err != nil {
				return err
			}
			if !eval.hasIssue || !eval.canRepair || strings.TrimSpace(eval.repairedPayload) == "" {
				continue
			}
			if _, err := s.execHook(tx,
				`UPDATE sync_mutations SET payload = ? WHERE target_key = ? AND project = ? AND seq = ? AND acked_at IS NULL`,
				eval.repairedPayload,
				DefaultSyncTargetKey,
				project,
				mutation.Seq,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLiteStore) evaluateCloudUpgradeLegacyMutations(project string) ([]cloudUpgradeLegacyMutationEvaluation, error) {
	return s.withReadTx(func(tx *sql.Tx) ([]cloudUpgradeLegacyMutationEvaluation, error) {
		mutations, err := s.listPendingProjectMutationsTx(tx, project)
		if err != nil {
			return nil, err
		}
		evaluations := make([]cloudUpgradeLegacyMutationEvaluation, 0, len(mutations))
		for _, mutation := range mutations {
			eval, err := s.evaluateCloudUpgradeLegacyMutationTx(tx, mutation)
			if err != nil {
				return nil, err
			}
			evaluations = append(evaluations, eval)
		}
		return evaluations, nil
	})
}

func (s *SQLiteStore) withReadTx(fn func(tx *sql.Tx) ([]cloudUpgradeLegacyMutationEvaluation, error)) ([]cloudUpgradeLegacyMutationEvaluation, error) {
	tx, err := s.beginTxHook()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return fn(tx)
}

func (s *SQLiteStore) listPendingProjectMutationsTx(tx *sql.Tx, project string) ([]SyncMutation, error) {
	rows, err := s.queryItHook(tx, `
		SELECT seq, target_key, entity, entity_key, op, payload, source, project, occurred_at, acked_at
		FROM sync_mutations
		WHERE target_key = ? AND project = ? AND acked_at IS NULL
		ORDER BY seq ASC
	`, DefaultSyncTargetKey, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	mutations := make([]SyncMutation, 0)
	for rows.Next() {
		var m SyncMutation
		if err := rows.Scan(&m.Seq, &m.TargetKey, &m.Entity, &m.EntityKey, &m.Op, &m.Payload, &m.Source, &m.Project, &m.OccurredAt, &m.AckedAt); err != nil {
			return nil, err
		}
		mutations = append(mutations, m)
	}
	return mutations, rows.Err()
}

func (s *SQLiteStore) evaluateCloudUpgradeLegacyMutationTx(tx *sql.Tx, mutation SyncMutation) (cloudUpgradeLegacyMutationEvaluation, error) {
	entity := strings.TrimSpace(mutation.Entity)
	op := strings.TrimSpace(mutation.Op)
	payload := strings.TrimSpace(mutation.Payload)
	base := CloudUpgradeLegacyMutationFinding{
		Seq:       mutation.Seq,
		Entity:    entity,
		Op:        op,
		EntityKey: strings.TrimSpace(mutation.EntityKey),
		TargetKey: strings.TrimSpace(mutation.TargetKey),
		Project:   strings.TrimSpace(mutation.Project),
	}

	repairable := func(msg, hint string, repairedPayload string) cloudUpgradeLegacyMutationEvaluation {
		finding := base
		finding.Repairable = true
		finding.ReasonCode = UpgradeReasonRepairableLegacyMutationPayload
		finding.Message = msg
		finding.RepairHint = hint
		return cloudUpgradeLegacyMutationEvaluation{finding: finding, hasIssue: true, canRepair: true, repairedPayload: repairedPayload}
	}
	blocked := func(code, msg string) cloudUpgradeLegacyMutationEvaluation {
		finding := base
		finding.Repairable = false
		finding.ReasonCode = code
		finding.Message = msg
		return cloudUpgradeLegacyMutationEvaluation{finding: finding, hasIssue: true, canRepair: false}
	}

	if payload == "" {
		return blocked(UpgradeReasonBlockedLegacyMutationManual, "legacy mutation payload is empty"), nil
	}

	supported := (entity == SyncEntitySession && (op == SyncOpUpsert || op == SyncOpDelete)) ||
		((entity == SyncEntityObservation || entity == SyncEntityPrompt) && (op == SyncOpUpsert || op == SyncOpDelete)) ||
		(entity == SyncEntityRelation && op == SyncOpUpsert)
	if !supported {
		return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("unsupported legacy mutation %q/%q", entity, op)), nil
	}

	switch entity {
	case SyncEntitySession:
		var body syncSessionPayload
		if err := decodeSyncPayload([]byte(payload), &body); err != nil {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("decode session payload: %v", err)), nil
		}
		body.ID = strings.TrimSpace(body.ID)
		body.Directory = strings.TrimSpace(body.Directory)
		changed := false
		if body.ID == "" && strings.TrimSpace(mutation.EntityKey) != "" {
			body.ID = strings.TrimSpace(mutation.EntityKey)
			changed = true
		}
		if body.ID == "" {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, "session payload id is required"), nil
		}
		if strings.TrimSpace(mutation.EntityKey) != "" && strings.TrimSpace(mutation.EntityKey) != body.ID {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("session entity_key %q does not match payload id %q", mutation.EntityKey, body.ID)), nil
		}
		if op == SyncOpUpsert && body.Directory == "" {
			var directory string
			err := tx.QueryRow(`SELECT ifnull(directory, '') FROM sessions WHERE id = ?`, body.ID).Scan(&directory)
			if errors.Is(err, sql.ErrNoRows) || strings.TrimSpace(directory) == "" {
				return blocked(UpgradeReasonBlockedLegacyMutationManual, "session payload directory is required and cannot be inferred from local state"), nil
			}
			if err != nil {
				return cloudUpgradeLegacyMutationEvaluation{}, err
			}
			body.Directory = strings.TrimSpace(directory)
			changed = true
		}
		if !changed {
			return cloudUpgradeLegacyMutationEvaluation{}, nil
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return cloudUpgradeLegacyMutationEvaluation{}, err
		}
		return repairable("session payload is missing required fields", "repair fills session id/directory from local sessions table", string(encoded)), nil

	case SyncEntityObservation:
		var body syncObservationPayload
		if err := decodeSyncPayload([]byte(payload), &body); err != nil {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("decode observation payload: %v", err)), nil
		}
		body.SyncID = strings.TrimSpace(body.SyncID)
		body.SessionID = strings.TrimSpace(body.SessionID)
		body.Type = strings.TrimSpace(body.Type)
		body.Title = strings.TrimSpace(body.Title)
		body.Content = strings.TrimSpace(body.Content)
		body.Scope = strings.TrimSpace(body.Scope)
		changed := false
		if body.SyncID == "" && strings.TrimSpace(mutation.EntityKey) != "" {
			body.SyncID = strings.TrimSpace(mutation.EntityKey)
			changed = true
		}
		if body.SyncID == "" {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, "observation payload sync_id is required"), nil
		}
		if strings.TrimSpace(mutation.EntityKey) != "" && strings.TrimSpace(mutation.EntityKey) != body.SyncID {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("observation entity_key %q does not match payload sync_id %q", mutation.EntityKey, body.SyncID)), nil
		}
		if op == SyncOpUpsert {
			obs, err := s.getObservationBySyncIDTx(tx, body.SyncID, true)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return cloudUpgradeLegacyMutationEvaluation{}, err
			}
			if strings.TrimSpace(body.SessionID) == "" && obs != nil && strings.TrimSpace(obs.SessionID) != "" {
				body.SessionID = strings.TrimSpace(obs.SessionID)
				changed = true
			}
			if strings.TrimSpace(body.Type) == "" && obs != nil && strings.TrimSpace(obs.Type) != "" {
				body.Type = strings.TrimSpace(obs.Type)
				changed = true
			}
			if strings.TrimSpace(body.Title) == "" && obs != nil && strings.TrimSpace(obs.Title) != "" {
				body.Title = strings.TrimSpace(obs.Title)
				changed = true
			}
			if strings.TrimSpace(body.Content) == "" && obs != nil && strings.TrimSpace(obs.Content) != "" {
				body.Content = strings.TrimSpace(obs.Content)
				changed = true
			}
			if strings.TrimSpace(body.Scope) == "" && obs != nil && strings.TrimSpace(obs.Scope) != "" {
				body.Scope = strings.TrimSpace(obs.Scope)
				changed = true
			}
			missing := []string{}
			if strings.TrimSpace(body.SessionID) == "" {
				missing = append(missing, "session_id")
			}
			if strings.TrimSpace(body.Type) == "" {
				missing = append(missing, "type")
			}
			if strings.TrimSpace(body.Title) == "" {
				missing = append(missing, "title")
			}
			if strings.TrimSpace(body.Content) == "" {
				missing = append(missing, "content")
			}
			if strings.TrimSpace(body.Scope) == "" {
				missing = append(missing, "scope")
			}
			if len(missing) > 0 {
				return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("observation payload missing required upsert fields: %s", strings.Join(missing, ", "))), nil
			}
		}
		if !changed {
			return cloudUpgradeLegacyMutationEvaluation{}, nil
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return cloudUpgradeLegacyMutationEvaluation{}, err
		}
		return repairable("observation payload is missing required fields for canonical bootstrap", "repair fills missing observation fields from local observations table", string(encoded)), nil

	case SyncEntityPrompt:
		var body syncPromptPayload
		if err := decodeSyncPayload([]byte(payload), &body); err != nil {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("decode prompt payload: %v", err)), nil
		}
		body.SyncID = strings.TrimSpace(body.SyncID)
		body.SessionID = strings.TrimSpace(body.SessionID)
		body.Content = strings.TrimSpace(body.Content)
		changed := false
		if body.SyncID == "" && strings.TrimSpace(mutation.EntityKey) != "" {
			body.SyncID = strings.TrimSpace(mutation.EntityKey)
			changed = true
		}
		if body.SyncID == "" {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, "prompt payload sync_id is required"), nil
		}
		if strings.TrimSpace(mutation.EntityKey) != "" && strings.TrimSpace(mutation.EntityKey) != body.SyncID {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("prompt entity_key %q does not match payload sync_id %q", mutation.EntityKey, body.SyncID)), nil
		}
		if op == SyncOpUpsert {
			var local syncPromptPayload
			err := tx.QueryRow(
				`SELECT sync_id, session_id, content, project, created_at FROM user_prompts WHERE sync_id = ? ORDER BY id DESC LIMIT 1`,
				body.SyncID,
			).Scan(&local.SyncID, &local.SessionID, &local.Content, &local.Project, &local.CreatedAt)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return cloudUpgradeLegacyMutationEvaluation{}, err
			}
			if strings.TrimSpace(body.SessionID) == "" && err == nil && strings.TrimSpace(local.SessionID) != "" {
				body.SessionID = strings.TrimSpace(local.SessionID)
				changed = true
			}
			if strings.TrimSpace(body.Content) == "" && err == nil && strings.TrimSpace(local.Content) != "" {
				body.Content = strings.TrimSpace(local.Content)
				changed = true
			}
			missing := []string{}
			if strings.TrimSpace(body.SessionID) == "" {
				missing = append(missing, "session_id")
			}
			if strings.TrimSpace(body.Content) == "" {
				missing = append(missing, "content")
			}
			if len(missing) > 0 {
				return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("prompt payload missing required upsert fields: %s", strings.Join(missing, ", "))), nil
			}
		}
		if !changed {
			return cloudUpgradeLegacyMutationEvaluation{}, nil
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return cloudUpgradeLegacyMutationEvaluation{}, err
		}
		return repairable("prompt payload is missing required fields for canonical bootstrap", "repair fills missing prompt fields from local prompts table", string(encoded)), nil

	case SyncEntityRelation:
		var body syncRelationPayload
		if err := decodeSyncPayload([]byte(payload), &body); err != nil {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("decode relation payload: %v", err)), nil
		}
		body.SyncID = strings.TrimSpace(body.SyncID)
		body.SourceID = strings.TrimSpace(body.SourceID)
		body.TargetID = strings.TrimSpace(body.TargetID)
		body.Relation = strings.TrimSpace(body.Relation)
		body.JudgmentStatus = strings.TrimSpace(body.JudgmentStatus)
		body.Project = strings.TrimSpace(body.Project)
		changed := false
		if body.SyncID == "" && strings.TrimSpace(mutation.EntityKey) != "" {
			body.SyncID = strings.TrimSpace(mutation.EntityKey)
			changed = true
		}
		if body.SyncID == "" {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, "relation payload sync_id is required"), nil
		}
		if strings.TrimSpace(mutation.EntityKey) != "" && strings.TrimSpace(mutation.EntityKey) != body.SyncID {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("relation entity_key %q does not match payload sync_id %q", mutation.EntityKey, body.SyncID)), nil
		}

		type localRelationPayload struct {
			SyncID         string
			SourceID       string
			TargetID       string
			Relation       string
			Reason         sql.NullString
			Evidence       sql.NullString
			Confidence     sql.NullFloat64
			JudgmentStatus string
			MarkedByActor  sql.NullString
			MarkedByKind   sql.NullString
			MarkedByModel  sql.NullString
			SessionID      sql.NullString
			CreatedAt      string
			UpdatedAt      string
		}
		var local localRelationPayload
		err := tx.QueryRow(`
			SELECT ifnull(sync_id, ''), ifnull(source_id, ''), ifnull(target_id, ''), ifnull(relation, ''),
			       reason, evidence, confidence, ifnull(judgment_status, ''), marked_by_actor,
			       marked_by_kind, marked_by_model, session_id, ifnull(created_at, ''), ifnull(updated_at, '')
			FROM memory_relations
			WHERE sync_id = ?
			ORDER BY id DESC LIMIT 1
		`, body.SyncID).Scan(
			&local.SyncID, &local.SourceID, &local.TargetID, &local.Relation,
			&local.Reason, &local.Evidence, &local.Confidence, &local.JudgmentStatus,
			&local.MarkedByActor, &local.MarkedByKind, &local.MarkedByModel, &local.SessionID,
			&local.CreatedAt, &local.UpdatedAt,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return cloudUpgradeLegacyMutationEvaluation{}, err
		}
		if err == nil {
			if body.SourceID == "" && strings.TrimSpace(local.SourceID) != "" {
				body.SourceID = strings.TrimSpace(local.SourceID)
				changed = true
			}
			if body.TargetID == "" && strings.TrimSpace(local.TargetID) != "" {
				body.TargetID = strings.TrimSpace(local.TargetID)
				changed = true
			}
			if body.Relation == "" && strings.TrimSpace(local.Relation) != "" {
				body.Relation = strings.TrimSpace(local.Relation)
				changed = true
			}
			if body.Reason == nil && local.Reason.Valid {
				v := local.Reason.String
				body.Reason = &v
				changed = true
			}
			if body.Evidence == nil && local.Evidence.Valid {
				v := local.Evidence.String
				body.Evidence = &v
				changed = true
			}
			if body.Confidence == nil && local.Confidence.Valid {
				v := local.Confidence.Float64
				body.Confidence = &v
				changed = true
			}
			if body.JudgmentStatus == "" && strings.TrimSpace(local.JudgmentStatus) != "" {
				body.JudgmentStatus = strings.TrimSpace(local.JudgmentStatus)
				changed = true
			}
			if (body.MarkedByActor == nil || strings.TrimSpace(*body.MarkedByActor) == "") && local.MarkedByActor.Valid && strings.TrimSpace(local.MarkedByActor.String) != "" {
				v := strings.TrimSpace(local.MarkedByActor.String)
				body.MarkedByActor = &v
				changed = true
			}
			if (body.MarkedByKind == nil || strings.TrimSpace(*body.MarkedByKind) == "") && local.MarkedByKind.Valid && strings.TrimSpace(local.MarkedByKind.String) != "" {
				v := strings.TrimSpace(local.MarkedByKind.String)
				body.MarkedByKind = &v
				changed = true
			}
			if body.MarkedByModel == nil && local.MarkedByModel.Valid {
				v := local.MarkedByModel.String
				body.MarkedByModel = &v
				changed = true
			}
			if body.SessionID == nil && local.SessionID.Valid {
				v := local.SessionID.String
				body.SessionID = &v
				changed = true
			}
			if strings.TrimSpace(body.CreatedAt) == "" && strings.TrimSpace(local.CreatedAt) != "" {
				body.CreatedAt = strings.TrimSpace(local.CreatedAt)
				changed = true
			}
			if strings.TrimSpace(body.UpdatedAt) == "" && strings.TrimSpace(local.UpdatedAt) != "" {
				body.UpdatedAt = strings.TrimSpace(local.UpdatedAt)
				changed = true
			}
		}
		if body.Project == "" && strings.TrimSpace(mutation.Project) != "" {
			body.Project = strings.TrimSpace(mutation.Project)
			changed = true
		}
		missing := []string{}
		if body.SourceID == "" {
			missing = append(missing, "source_id")
		}
		if body.TargetID == "" {
			missing = append(missing, "target_id")
		}
		if body.Relation == "" {
			missing = append(missing, "relation")
		}
		if body.JudgmentStatus == "" {
			missing = append(missing, "judgment_status")
		}
		if body.MarkedByActor == nil || strings.TrimSpace(*body.MarkedByActor) == "" {
			missing = append(missing, "marked_by_actor")
		}
		if body.MarkedByKind == nil || strings.TrimSpace(*body.MarkedByKind) == "" {
			missing = append(missing, "marked_by_kind")
		}
		if body.Project == "" {
			missing = append(missing, "project")
		}
		if len(missing) > 0 {
			return blocked(UpgradeReasonBlockedLegacyMutationManual, fmt.Sprintf("relation payload missing required upsert fields: %s", strings.Join(missing, ", "))), nil
		}
		if !changed {
			return cloudUpgradeLegacyMutationEvaluation{}, nil
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return cloudUpgradeLegacyMutationEvaluation{}, err
		}
		return repairable("relation payload is missing required fields for canonical bootstrap", "repair fills missing relation fields from local memory_relations table and mutation project", string(encoded)), nil
	}

	return cloudUpgradeLegacyMutationEvaluation{}, nil
}

func (s *SQLiteStore) cloudUpgradeManualActionReport(project string) (bool, CloudUpgradeRepairReport, error) {
	targetKey := DefaultSyncTargetKey
	if project != "" {
		targetKey = fmt.Sprintf("%s:%s", DefaultSyncTargetKey, project)
	}
	state, err := s.GetSyncState(targetKey)
	if err != nil {
		return false, CloudUpgradeRepairReport{}, fmt.Errorf("read sync state for cloud upgrade repair: %w", err)
	}
	if state == nil {
		return false, CloudUpgradeRepairReport{}, nil
	}
	reasonCode := strings.TrimSpace(derefString(state.ReasonCode))
	if reasonCode == "" {
		return false, CloudUpgradeRepairReport{}, nil
	}

	reasonMap := map[string]string{
		"auth_required":      "upgrade_policy_auth_required",
		"policy_forbidden":   "upgrade_policy_forbidden",
		"cloud_config_error": "upgrade_policy_cloud_config_error",
	}
	repairReasonCode, requiresManualAction := reasonMap[reasonCode]
	if !requiresManualAction {
		return false, CloudUpgradeRepairReport{}, nil
	}
	reasonMessage := strings.TrimSpace(derefString(state.ReasonMessage))
	if reasonMessage == "" {
		reasonMessage = "cloud policy/auth precondition must be resolved before repair"
	}
	return true, CloudUpgradeRepairReport{
		Class:      UpgradeRepairClassPolicy,
		ReasonCode: repairReasonCode,
		Message:    fmt.Sprintf("manual-action-required: %s", reasonMessage),
		Applied:    false,
	}, nil
}

func (s *SQLiteStore) projectSyncBackfillRequired(project string) (bool, error) {
	var missing int
	err := s.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM sessions sess
			WHERE sess.project = ?
			  AND NOT EXISTS (
				SELECT 1 FROM sync_mutations sm
				WHERE sm.target_key = ?
				  AND sm.entity = ?
				  AND sm.entity_key = sess.id
				  AND sm.source = ?
			  )
			UNION ALL
			SELECT 1
			FROM observations obs
			LEFT JOIN sessions sess ON sess.id = obs.session_id
			WHERE (
				ifnull(obs.project, '') = ?
				OR (ifnull(obs.project, '') = '' AND ifnull(sess.project, '') = ?)
			)
			  AND obs.deleted_at IS NULL
			  AND NOT EXISTS (
				SELECT 1 FROM sync_mutations sm
				WHERE sm.target_key = ?
				  AND sm.entity = ?
				  AND sm.entity_key = obs.sync_id
				  AND sm.source = ?
			  )
		)
	`, project, DefaultSyncTargetKey, SyncEntitySession, SyncSourceLocal, project, project, DefaultSyncTargetKey, SyncEntityObservation, SyncSourceLocal).Scan(&missing)
	if err != nil {
		return false, fmt.Errorf("detect project sync metadata gaps: %w", err)
	}
	return missing == 1, nil
}

func normalizeUpgradeStage(stage string) string {
	stage = strings.TrimSpace(strings.ToLower(stage))
	switch stage {
	case UpgradeStagePlanned,
		UpgradeStageDoctorReady,
		UpgradeStageDoctorBlocked,
		UpgradeStageRepairApplied,
		UpgradeStageBootstrapEnrolled,
		UpgradeStageBootstrapPushed,
		UpgradeStageBootstrapVerified,
		UpgradeStageRolledBack:
		return stage
	default:
		return UpgradeStagePlanned
	}
}

func normalizeUpgradeRepairClass(class string) string {
	class = strings.TrimSpace(strings.ToLower(class))
	switch class {
	case UpgradeRepairClassNone,
		UpgradeRepairClassReady,
		UpgradeRepairClassRepairable,
		UpgradeRepairClassBlocked,
		UpgradeRepairClassPolicy:
		return class
	default:
		return UpgradeRepairClassNone
	}
}

func (s *SQLiteStore) migrateFTSTopicKey() error {
	var colCount int
	err := s.db.QueryRow("SELECT COUNT(*) FROM pragma_table_xinfo('observations_fts') WHERE name = 'topic_key'").Scan(&colCount)
	if err != nil || colCount > 0 {
		return nil
	}

	if _, err := s.execHook(s.db, `
		DROP TRIGGER IF EXISTS obs_fts_insert;
		DROP TRIGGER IF EXISTS obs_fts_update;
		DROP TRIGGER IF EXISTS obs_fts_delete;
		DROP TABLE IF EXISTS observations_fts;
		CREATE VIRTUAL TABLE observations_fts USING fts5(
			title,
			content,
			tool_name,
			type,
			project,
			topic_key,
			content='observations',
			content_rowid='id'
		);
		INSERT INTO observations_fts(rowid, title, content, tool_name, type, project, topic_key)
		SELECT id, title, content, tool_name, type, project, topic_key
		FROM observations
		WHERE deleted_at IS NULL;

		CREATE TRIGGER obs_fts_insert AFTER INSERT ON observations BEGIN
			INSERT INTO observations_fts(rowid, title, content, tool_name, type, project, topic_key)
			VALUES (new.id, new.title, new.content, new.tool_name, new.type, new.project, new.topic_key);
		END;

		CREATE TRIGGER obs_fts_delete AFTER DELETE ON observations BEGIN
			INSERT INTO observations_fts(observations_fts, rowid, title, content, tool_name, type, project, topic_key)
			VALUES ('delete', old.id, old.title, old.content, old.tool_name, old.type, old.project, old.topic_key);
		END;

		CREATE TRIGGER obs_fts_update AFTER UPDATE ON observations BEGIN
			INSERT INTO observations_fts(observations_fts, rowid, title, content, tool_name, type, project, topic_key)
			VALUES ('delete', old.id, old.title, old.content, old.tool_name, old.type, old.project, old.topic_key);
			INSERT INTO observations_fts(rowid, title, content, tool_name, type, project, topic_key)
			VALUES (new.id, new.title, new.content, new.tool_name, new.type, new.project, new.topic_key);
		END;
	`); err != nil {
		return fmt.Errorf("migrate fts topic_key: %w", err)
	}
	return nil
}

// ─── Sessions ────────────────────────────────────────────────────────────────

func (s *SQLiteStore) CreateSession(id, project, directory string) error {
	// Normalize project name before storing
	project, _ = NormalizeProject(project)

	return s.withTx(func(tx *sql.Tx) error {
		if err := s.createSessionTx(tx, id, project, directory); err != nil {
			return err
		}
		var startedAt string
		if err := tx.QueryRow(`SELECT started_at FROM sessions WHERE id = ?`, id).Scan(&startedAt); err != nil {
			return err
		}
		return s.enqueueSyncMutationTx(tx, SyncEntitySession, id, SyncOpUpsert, syncSessionPayload{
			ID:        id,
			Project:   project,
			Directory: directory,
			StartedAt: startedAt,
		})
	})
}

func (s *SQLiteStore) EndSession(id string, summary string) error {
	return s.withTx(func(tx *sql.Tx) error {
		res, err := s.execHook(tx,
			`UPDATE sessions SET ended_at = datetime('now'), summary = ? WHERE id = ?`,
			nullableString(summary), id,
		)
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			return nil
		}

		var startedAt, endedAt string
		var project, directory string
		var storedSummary *string
		if err := tx.QueryRow(
			`SELECT project, directory, started_at, ended_at, summary FROM sessions WHERE id = ?`,
			id,
		).Scan(&project, &directory, &startedAt, &endedAt, &storedSummary); err != nil {
			return err
		}

		return s.enqueueSyncMutationTx(tx, SyncEntitySession, id, SyncOpUpsert, syncSessionPayload{
			ID:        id,
			Project:   project,
			Directory: directory,
			StartedAt: startedAt,
			EndedAt:   &endedAt,
			Summary:   storedSummary,
		})
	})
}

func (s *SQLiteStore) GetSession(id string) (*Session, error) {
	row := s.db.QueryRow(
		`SELECT id, project, directory, started_at, ended_at, summary FROM sessions WHERE id = ?`, id,
	)
	var sess Session
	if err := row.Scan(&sess.ID, &sess.Project, &sess.Directory, &sess.StartedAt, &sess.EndedAt, &sess.Summary); err != nil {
		return nil, err
	}
	return &sess, nil
}

// MostRecentActiveSession resolves the active (un-ended) session for a project
// from the persisted sessions table. It returns the session ID and ok=true when
// such a session exists, or ok=false when none does.
//
// This is the cross-process resolution that fixes issue #386: the SessionStart
// hook registers a UUID session via the HTTP server (POST /sessions) in one
// process, while mem_save runs in the separate MCP (stdio) process. The two
// share only the SQLite store, so the active session must be read from disk —
// never from in-memory state.
//
// Selection rules:
//   - Scope to the (normalized) project.
//   - Require ended_at IS NULL — ended sessions are never returned, so stale
//     sessions naturally fall out without any explicit clearing step.
//   - Exclude the manual-save fallback sessions (id LIKE 'manual-save%'); those
//     are created by the fallback path itself and must not be resolved as "the
//     active session", which would make resolution circular.
//   - When multiple un-ended sessions exist, pick the MOST RECENT by
//     started_at DESC, with id DESC as a deterministic tie-breaker.
func (s *SQLiteStore) MostRecentActiveSession(project string) (string, bool, error) {
	project, _ = NormalizeProject(project)
	if project == "" {
		return "", false, nil
	}

	var id string
	err := s.db.QueryRow(`
		SELECT id
		FROM sessions
		WHERE LOWER(project) = ?
		  AND ended_at IS NULL
		  AND id NOT LIKE 'manual-save%'
		ORDER BY datetime(started_at) DESC, id DESC
		LIMIT 1
	`, project).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

func (s *SQLiteStore) RecentSessions(project string, limit int) ([]SessionSummary, error) {
	// Normalize project filter for case-insensitive matching
	project, _ = NormalizeProject(project)

	if limit <= 0 {
		limit = 5
	}

	query := `
		SELECT s.id, s.project, s.started_at, s.ended_at, s.summary,
		       COUNT(o.id) as observation_count
		FROM sessions s
		LEFT JOIN observations o ON o.session_id = s.id AND o.deleted_at IS NULL
		WHERE 1=1
	`
	args := []any{}

	if project != "" {
		query += " AND LOWER(s.project) = ?"
		args = append(args, project)
	}

	query += " GROUP BY s.id ORDER BY MAX(datetime(COALESCE(o.created_at, s.started_at))) DESC, s.id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.queryItHook(s.db, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SessionSummary
	for rows.Next() {
		var ss SessionSummary
		if err := rows.Scan(&ss.ID, &ss.Project, &ss.StartedAt, &ss.EndedAt, &ss.Summary, &ss.ObservationCount); err != nil {
			return nil, err
		}
		results = append(results, ss)
	}
	return results, rows.Err()
}

// AllSessions returns recent sessions ordered by most recent first (for TUI browsing).
func (s *SQLiteStore) AllSessions(project string, limit int) ([]SessionSummary, error) {
	if limit <= 0 {
		limit = 50
	}

	query := `
		SELECT s.id, s.project, s.started_at, s.ended_at, s.summary,
		       COUNT(o.id) as observation_count
		FROM sessions s
		LEFT JOIN observations o ON o.session_id = s.id AND o.deleted_at IS NULL
		WHERE 1=1
	`
	args := []any{}

	if project != "" {
		query += " AND s.project = ?"
		args = append(args, project)
	}

	query += " GROUP BY s.id ORDER BY MAX(datetime(COALESCE(o.created_at, s.started_at))) DESC, s.id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.queryItHook(s.db, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SessionSummary
	for rows.Next() {
		var ss SessionSummary
		if err := rows.Scan(&ss.ID, &ss.Project, &ss.StartedAt, &ss.EndedAt, &ss.Summary, &ss.ObservationCount); err != nil {
			return nil, err
		}
		results = append(results, ss)
	}
	return results, rows.Err()
}

// AllObservations returns recent observations ordered by most recent first (for TUI browsing).
func (s *SQLiteStore) AllObservations(project, scope string, limit int) ([]Observation, error) {
	if limit <= 0 {
		limit = s.cfg.MaxContextResults
	}

	query := `
		SELECT ` + observationSelectColumns + `
		FROM observations o
		WHERE o.deleted_at IS NULL
	`
	args := []any{}

	if project != "" {
		query += " AND o.project = ?"
		args = append(args, project)
	}
	if scope != "" {
		query += " AND o.scope = ?"
		args = append(args, normalizeScope(scope))
	}

	query += " ORDER BY datetime(o.created_at) DESC, o.id DESC LIMIT ?"
	args = append(args, limit)

	return s.queryObservations(query, args...)
}

// SessionObservations returns all observations for a specific session.
func (s *SQLiteStore) SessionObservations(sessionID string, limit int) ([]Observation, error) {
	if limit <= 0 {
		limit = 200
	}

	query := `
		SELECT ` + observationSelectColumns + `
		FROM observations
		WHERE session_id = ? AND deleted_at IS NULL
		ORDER BY created_at ASC
		LIMIT ?
	`
	return s.queryObservations(query, sessionID, limit)
}

// ─── Observations ────────────────────────────────────────────────────────────

func (s *SQLiteStore) AddObservation(p AddObservationParams) (int64, error) {
	// Normalize project name (lowercase + trim) before any persistence
	p.Project, _ = NormalizeProject(p.Project)

	// Strip <private>...</private> tags before persisting ANYTHING
	title := stripPrivateTags(p.Title)
	content := stripPrivateTags(p.Content)

	if len(content) > s.cfg.MaxObservationLength {
		content = content[:s.cfg.MaxObservationLength] + "... [truncated]"
	}
	scope := normalizeScope(p.Scope)
	normHash := hashNormalized(content)
	topicKey := normalizeTopicKey(p.TopicKey)

	var observationID int64
	err := s.withTx(func(tx *sql.Tx) error {
		var obs *Observation
		if topicKey != "" {
			var existingID int64
			err := tx.QueryRow(
				`SELECT id FROM observations
				 WHERE topic_key = ?
				   AND ifnull(project, '') = ifnull(?, '')
				   AND scope = ?
				   AND deleted_at IS NULL
				 ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC
				 LIMIT 1`,
				topicKey, nullableString(p.Project), scope,
			).Scan(&existingID)
			if err == nil {
				if _, err := s.execHook(tx,
					`UPDATE observations
					 SET type = ?,
					     title = ?,
					     content = ?,
					     tool_name = ?,
					     topic_key = ?,
					     normalized_hash = ?,
					     revision_count = revision_count + 1,
					     last_seen_at = datetime('now'),
					     updated_at = datetime('now')
					 WHERE id = ?`,
					p.Type,
					title,
					content,
					nullableString(p.ToolName),
					nullableString(topicKey),
					normHash,
					existingID,
				); err != nil {
					return err
				}
				obs, err = s.getObservationTx(tx, existingID)
				if err != nil {
					return err
				}
				observationID = existingID
				return s.enqueueSyncMutationTx(tx, SyncEntityObservation, obs.SyncID, SyncOpUpsert, observationPayloadFromObservation(obs))
			}
			if err != sql.ErrNoRows {
				return err
			}
		}

		window := dedupeWindowExpression(s.cfg.DedupeWindow)
		var existingID int64
		err := tx.QueryRow(
			`SELECT id FROM observations
			 WHERE normalized_hash = ?
			   AND ifnull(project, '') = ifnull(?, '')
			   AND scope = ?
			   AND type = ?
			   AND title = ?
			   AND deleted_at IS NULL
			   AND datetime(created_at) >= datetime('now', ?)
			 ORDER BY created_at DESC
			 LIMIT 1`,
			normHash, nullableString(p.Project), scope, p.Type, title, window,
		).Scan(&existingID)
		if err == nil {
			if _, err := s.execHook(tx,
				`UPDATE observations
				 SET duplicate_count = duplicate_count + 1,
				     last_seen_at = datetime('now'),
				     updated_at = datetime('now')
				 WHERE id = ?`,
				existingID,
			); err != nil {
				return err
			}
			obs, err = s.getObservationTx(tx, existingID)
			if err != nil {
				return err
			}
			observationID = existingID
			return s.enqueueSyncMutationTx(tx, SyncEntityObservation, obs.SyncID, SyncOpUpsert, observationPayloadFromObservation(obs))
		}
		if err != sql.ErrNoRows {
			return err
		}

		syncID := newSyncID("obs")
		res, err := s.execHook(tx,
			`INSERT INTO observations (sync_id, session_id, type, title, content, tool_name, project, scope, topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, datetime('now'), datetime('now'))`,
			syncID, p.SessionID, p.Type, title, content,
			nullableString(p.ToolName), nullableString(p.Project), scope, nullableString(topicKey), normHash,
		)
		if err != nil {
			return err
		}
		observationID, err = res.LastInsertId()
		if err != nil {
			return err
		}

		// Populate review_after for types that have a configured decay offset.
		// expires_at is intentionally NULL for all types in Phase 1.
		// This UPDATE runs only for NEW inserts (not topic_key revisions or deduplication).
		if months, ok := decayReviewAfterMonths[p.Type]; ok {
			reviewAfter := time.Now().UTC().AddDate(0, months, 0).Format("2006-01-02 15:04:05")
			if _, err := s.execHook(tx,
				`UPDATE observations SET review_after = ? WHERE id = ?`,
				reviewAfter, observationID,
			); err != nil {
				return fmt.Errorf("set review_after: %w", err)
			}
		}

		obs, err = s.getObservationTx(tx, observationID)
		if err != nil {
			return err
		}
		return s.enqueueSyncMutationTx(tx, SyncEntityObservation, obs.SyncID, SyncOpUpsert, observationPayloadFromObservation(obs))
	})
	if err != nil {
		return 0, err
	}
	return observationID, nil
}

func (s *SQLiteStore) RecentObservations(project, scope string, limit int) ([]Observation, error) {
	// Normalize project filter for case-insensitive matching
	project, _ = NormalizeProject(project)

	if limit <= 0 {
		limit = s.cfg.MaxContextResults
	}

	query := `
		SELECT ` + observationSelectColumns + `
		FROM observations o
		WHERE o.deleted_at IS NULL
	`
	args := []any{}

	if project != "" {
		query += " AND LOWER(o.project) = ?"
		args = append(args, project)
	}
	if scope != "" {
		query += " AND o.scope = ?"
		args = append(args, normalizeScope(scope))
	}

	query += " ORDER BY datetime(o.created_at) DESC, o.id DESC LIMIT ?"
	args = append(args, limit)

	return s.queryObservations(query, args...)
}

func (s *SQLiteStore) PinnedObservations(project, scope string) ([]Observation, error) {
	project, _ = NormalizeProject(project)

	query := `
		SELECT ` + observationSelectColumns + `
		FROM observations o
		WHERE o.deleted_at IS NULL AND o.pinned = 1
	`
	args := []any{}

	if project != "" {
		query += " AND LOWER(o.project) = ?"
		args = append(args, project)
	}
	if scope != "" {
		query += " AND o.scope = ?"
		args = append(args, normalizeScope(scope))
	}

	query += " ORDER BY datetime(o.created_at) DESC, o.id DESC"
	return s.queryObservations(query, args...)
}

func (s *SQLiteStore) PinObservation(id int64) error {
	return s.setObservationPinned(id, true)
}

func (s *SQLiteStore) UnpinObservation(id int64) error {
	return s.setObservationPinned(id, false)
}

func (s *SQLiteStore) setObservationPinned(id int64, pinned bool) error {
	value := 0
	if pinned {
		value = 1
	}
	res, err := s.execHook(s.db, `UPDATE observations SET pinned = ? WHERE id = ? AND deleted_at IS NULL`, value, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrObservationNotFound
	}
	return nil
}

func (s *SQLiteStore) recentUnpinnedObservations(project, scope string, limit int) ([]Observation, error) {
	project, _ = NormalizeProject(project)
	if limit <= 0 {
		limit = s.cfg.MaxContextResults
	}

	query := `
		SELECT ` + observationSelectColumns + `
		FROM observations o
		WHERE o.deleted_at IS NULL AND o.pinned = 0
	`
	args := []any{}
	if project != "" {
		query += " AND LOWER(o.project) = ?"
		args = append(args, project)
	}
	if scope != "" {
		query += " AND o.scope = ?"
		args = append(args, normalizeScope(scope))
	}
	query += " ORDER BY datetime(o.created_at) DESC, o.id DESC LIMIT ?"
	args = append(args, limit)
	return s.queryObservations(query, args...)
}

// ObservationsNeedingReview returns non-deleted observations whose review_after has passed.
// An empty project searches all projects, matching existing browse/search conventions.
func (s *SQLiteStore) ObservationsNeedingReview(project string, limit int) ([]Observation, error) {
	project, _ = NormalizeProject(project)
	if limit <= 0 {
		limit = s.cfg.MaxContextResults
	}
	query := `
		SELECT ` + observationSelectColumns + `
		FROM observations o
		WHERE o.deleted_at IS NULL
		  AND o.review_after IS NOT NULL
		  AND datetime(o.review_after) <= datetime('now')
	`
	args := []any{}
	if project != "" {
		query += " AND LOWER(o.project) = ?"
		args = append(args, project)
	}
	query += " ORDER BY datetime(o.review_after) ASC, o.id ASC LIMIT ?"
	args = append(args, limit)

	return s.queryObservations(query, args...)
}

// MarkReviewed resets an observation's review_after using its type's configured decay offset.
// Types without a decay offset return to a NULL review_after value.
// This lifecycle reset is intentionally local-only until the sync wire format includes review_after.
func (s *SQLiteStore) MarkReviewed(id int64) error {
	return s.withTx(func(tx *sql.Tx) error {
		obs, err := s.getObservationTx(tx, id)
		if err == sql.ErrNoRows {
			return ErrObservationNotFound
		}
		if err != nil {
			return err
		}

		var reviewAfter any
		if months, ok := decayReviewAfterMonths[obs.Type]; ok {
			reviewAfter = time.Now().UTC().AddDate(0, months, 0).Format("2006-01-02 15:04:05")
		}
		if _, err := s.execHook(tx, `UPDATE observations SET review_after = ?, updated_at = datetime('now') WHERE id = ? AND deleted_at IS NULL`, reviewAfter, id); err != nil {
			return err
		}
		return nil
	})
}

// ─── User Prompts ────────────────────────────────────────────────────────────

func (s *SQLiteStore) AddPrompt(p AddPromptParams) (int64, error) {
	// Normalize project name before storing
	p.Project, _ = NormalizeProject(p.Project)

	content := s.preparePromptContent(p.Content)

	var promptID int64
	err := s.withTx(func(tx *sql.Tx) error {
		syncID := newSyncID("prompt")
		res, err := s.execHook(tx,
			`INSERT INTO user_prompts (sync_id, session_id, content, project) VALUES (?, ?, ?, ?)`,
			syncID, p.SessionID, content, nullableString(p.Project),
		)
		if err != nil {
			return err
		}
		promptID, err = res.LastInsertId()
		if err != nil {
			return err
		}
		var createdAt string
		if err := tx.QueryRow(`SELECT created_at FROM user_prompts WHERE id = ?`, promptID).Scan(&createdAt); err != nil {
			return err
		}
		if _, err := s.execHook(tx, `DELETE FROM prompt_tombstones WHERE sync_id = ?`, syncID); err != nil {
			return err
		}
		return s.enqueueSyncMutationTx(tx, SyncEntityPrompt, syncID, SyncOpUpsert, syncPromptPayload{
			SyncID:    syncID,
			SessionID: p.SessionID,
			Content:   content,
			Project:   nullableString(p.Project),
			CreatedAt: createdAt,
		})
	})
	if err != nil {
		return 0, err
	}
	return promptID, nil
}

func (s *SQLiteStore) AddPromptIfMissing(p AddPromptParams) (int64, bool, error) {
	p.Project, _ = NormalizeProject(p.Project)
	content := s.preparePromptContent(p.Content)

	var promptID int64
	inserted := false
	err := s.withTx(func(tx *sql.Tx) error {
		err := tx.QueryRow(
			`SELECT id FROM user_prompts WHERE session_id = ? AND ifnull(project, '') = ? AND content = ? ORDER BY id DESC LIMIT 1`,
			p.SessionID, p.Project, content,
		).Scan(&promptID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		syncID := newSyncID("prompt")
		res, err := s.execHook(tx,
			`INSERT INTO user_prompts (sync_id, session_id, content, project) VALUES (?, ?, ?, ?)`,
			syncID, p.SessionID, content, nullableString(p.Project),
		)
		if err != nil {
			return err
		}
		promptID, err = res.LastInsertId()
		if err != nil {
			return err
		}
		var createdAt string
		if err := tx.QueryRow(`SELECT created_at FROM user_prompts WHERE id = ?`, promptID).Scan(&createdAt); err != nil {
			return err
		}
		if _, err := s.execHook(tx, `DELETE FROM prompt_tombstones WHERE sync_id = ?`, syncID); err != nil {
			return err
		}
		inserted = true
		return s.enqueueSyncMutationTx(tx, SyncEntityPrompt, syncID, SyncOpUpsert, syncPromptPayload{
			SyncID:    syncID,
			SessionID: p.SessionID,
			Content:   content,
			Project:   nullableString(p.Project),
			CreatedAt: createdAt,
		})
	})
	if err != nil {
		return 0, false, err
	}
	return promptID, inserted, nil
}

func (s *SQLiteStore) preparePromptContent(content string) string {
	content = stripPrivateTags(content)
	if len(content) > s.cfg.MaxObservationLength {
		content = content[:s.cfg.MaxObservationLength] + "... [truncated]"
	}
	return content
}

func (s *SQLiteStore) RecentPrompts(project string, limit int) ([]Prompt, error) {
	// Normalize project filter for case-insensitive matching
	project, _ = NormalizeProject(project)

	if limit <= 0 {
		limit = 20
	}

	query := `SELECT id, ifnull(sync_id, '') as sync_id, session_id, content, ifnull(project, '') as project, created_at FROM user_prompts`
	args := []any{}

	if project != "" {
		query += " WHERE project = ?"
		args = append(args, project)
	}

	query += " ORDER BY created_at DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.queryItHook(s.db, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Prompt
	for rows.Next() {
		var p Prompt
		if err := rows.Scan(&p.ID, &p.SyncID, &p.SessionID, &p.Content, &p.Project, &p.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, p)
	}
	return results, rows.Err()
}

func (s *SQLiteStore) SearchPrompts(query string, project string, limit int) ([]Prompt, error) {
	if limit <= 0 {
		limit = 10
	}

	ftsQuery := sanitizeFTS(query)

	sql := `
		SELECT p.id, ifnull(p.sync_id, '') as sync_id, p.session_id, p.content, ifnull(p.project, '') as project, p.created_at
		FROM prompts_fts fts
		JOIN user_prompts p ON p.id = fts.rowid
		WHERE prompts_fts MATCH ?
	`
	args := []any{ftsQuery}

	if project != "" {
		sql += " AND p.project = ?"
		args = append(args, project)
	}

	sql += " ORDER BY fts.rank LIMIT ?"
	args = append(args, limit)

	rows, err := s.queryItHook(s.db, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("search prompts: %w", err)
	}
	defer rows.Close()

	var results []Prompt
	for rows.Next() {
		var p Prompt
		if err := rows.Scan(&p.ID, &p.SyncID, &p.SessionID, &p.Content, &p.Project, &p.CreatedAt); err != nil {
			return nil, err
		}
		results = append(results, p)
	}
	return results, rows.Err()
}

// ─── Delete Session ──────────────────────────────────────────────────────────

// DeleteSession hard-deletes a session and its prompts.
// It returns ErrSessionHasObservations if the session has any observations
// (including soft-deleted ones) to prevent orphaned rows.
// It returns ErrSessionNotFound if no session with that ID exists.
//
// When the session belongs to an enrolled project, this operation also enqueues
// a session/delete mutation so cloud replicas can remove the session.
func (s *SQLiteStore) DeleteSession(id string) error {
	return s.withTx(func(tx *sql.Tx) error {
		var project string
		if err := tx.QueryRow(`SELECT project FROM sessions WHERE id = ?`, id).Scan(&project); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: %q", ErrSessionNotFound, id)
			}
			return fmt.Errorf("delete session: load session: %w", err)
		}

		var enrolled int
		if err := tx.QueryRow(`SELECT 1 FROM sync_enrolled_projects WHERE project = ? LIMIT 1`, project).Scan(&enrolled); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("delete session: check enrollment: %w", err)
			}
		}
		// Count ALL observations for the session, including soft-deleted ones,
		// because the FK constraint on observations.session_id has no ON DELETE CASCADE.
		var count int
		rows, err := s.queryItHook(tx, `SELECT COUNT(*) FROM observations WHERE session_id = ?`, id)
		if err != nil {
			return fmt.Errorf("delete session: count observations: %w", err)
		}
		if rows.Next() {
			if err := rows.Scan(&count); err != nil {
				_ = rows.Close()
				return fmt.Errorf("delete session: count observations: %w", err)
			}
		}
		_ = rows.Close()
		if count > 0 {
			return fmt.Errorf("%w: session %q has %d observation(s)", ErrSessionHasObservations, id, count)
		}

		if _, err := s.execHook(tx, `DELETE FROM user_prompts WHERE session_id = ?`, id); err != nil {
			return fmt.Errorf("delete session: remove prompts: %w", err)
		}

		res, err := s.execHook(tx, `DELETE FROM sessions WHERE id = ?`, id)
		if err != nil {
			var sqliteErr *sqlite.Error
			if errors.As(err, &sqliteErr) && sqliteErr.Code() == sqliteConstraintForeignKey {
				return fmt.Errorf("%w: session %q has observation(s)", ErrSessionHasObservations, id)
			}
			return fmt.Errorf("delete session: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("delete session: rows affected: %w", err)
		}
		if n == 0 {
			return fmt.Errorf("%w: %q", ErrSessionNotFound, id)
		}

		if enrolled == 1 {
			now := Now()
			if err := s.enqueueSyncMutationTx(tx, SyncEntitySession, id, SyncOpDelete, syncSessionPayload{
				ID:        id,
				Project:   project,
				DeletedAt: &now,
			}); err != nil {
				return fmt.Errorf("delete session: enqueue mutation: %w", err)
			}
		}

		return nil
	})
}

// ─── Delete Prompt ───────────────────────────────────────────────────────────

// DeletePrompt hard-deletes a single prompt by ID and records a sync tombstone.
// It returns ErrPromptNotFound if no prompt with that ID exists.
func (s *SQLiteStore) DeletePrompt(id int64) error {
	return s.withTx(func(tx *sql.Tx) error {
		var payload syncPromptPayload
		var project string
		if err := tx.QueryRow(`SELECT sync_id, session_id, ifnull(project, '') FROM user_prompts WHERE id = ?`, id).Scan(&payload.SyncID, &payload.SessionID, &project); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: prompt #%d", ErrPromptNotFound, id)
			}
			return fmt.Errorf("delete prompt: load row: %w", err)
		}
		payload.Project = nullableString(project)
		now := Now()
		payload.Deleted = true
		payload.HardDelete = true
		payload.DeletedAt = &now

		res, err := s.execHook(tx, `DELETE FROM user_prompts WHERE id = ?`, id)
		if err != nil {
			return fmt.Errorf("delete prompt: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("delete prompt: rows affected: %w", err)
		}
		if n == 0 {
			return fmt.Errorf("%w: prompt #%d", ErrPromptNotFound, id)
		}
		if _, err := s.execHook(tx,
			`INSERT INTO prompt_tombstones (sync_id, session_id, project, deleted_at)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(sync_id) DO UPDATE SET session_id = excluded.session_id, project = excluded.project, deleted_at = excluded.deleted_at`,
			payload.SyncID, payload.SessionID, payload.Project, now,
		); err != nil {
			return fmt.Errorf("delete prompt: upsert tombstone: %w", err)
		}
		if err := s.enqueueSyncMutationTx(tx, SyncEntityPrompt, payload.SyncID, SyncOpDelete, payload); err != nil {
			return fmt.Errorf("delete prompt: enqueue mutation: %w", err)
		}
		return nil
	})
}

// ─── Get Single Observation ──────────────────────────────────────────────────

func (s *SQLiteStore) GetObservation(id int64) (*Observation, error) {
	row := s.db.QueryRow(
		`SELECT `+observationSelectColumns+`
		 FROM observations WHERE id = ? AND deleted_at IS NULL`, id,
	)
	var o Observation
	if err := scanObservationRowSQLite(row, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

func (s *SQLiteStore) UpdateObservation(id int64, p UpdateObservationParams) (*Observation, error) {
	var updated *Observation
	err := s.withTx(func(tx *sql.Tx) error {
		obs, err := s.getObservationTx(tx, id)
		if err != nil {
			return err
		}

		typ := obs.Type
		title := obs.Title
		content := obs.Content
		project := derefString(obs.Project)
		scope := obs.Scope
		topicKey := derefString(obs.TopicKey)

		if p.Type != nil {
			typ = *p.Type
		}
		if p.Title != nil {
			title = stripPrivateTags(*p.Title)
		}
		if p.Content != nil {
			content = stripPrivateTags(*p.Content)
			if len(content) > s.cfg.MaxObservationLength {
				content = content[:s.cfg.MaxObservationLength] + "... [truncated]"
			}
		}
		if p.Project != nil {
			project, _ = NormalizeProject(*p.Project)
		}
		if p.Scope != nil {
			scope = normalizeScope(*p.Scope)
		}
		if p.TopicKey != nil {
			topicKey = normalizeTopicKey(*p.TopicKey)
		}

		if _, err := s.execHook(tx,
			`UPDATE observations
			 SET type = ?,
			     title = ?,
			     content = ?,
			     project = ?,
			     scope = ?,
			     topic_key = ?,
			     normalized_hash = ?,
			     revision_count = revision_count + 1,
			     updated_at = datetime('now')
			 WHERE id = ? AND deleted_at IS NULL`,
			typ,
			title,
			content,
			nullableString(project),
			scope,
			nullableString(topicKey),
			hashNormalized(content),
			id,
		); err != nil {
			return err
		}

		updated, err = s.getObservationTx(tx, id)
		if err != nil {
			return err
		}
		return s.enqueueSyncMutationTx(tx, SyncEntityObservation, updated.SyncID, SyncOpUpsert, observationPayloadFromObservation(updated))
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *SQLiteStore) DeleteObservation(id int64, hardDelete bool) error {
	return s.withTx(func(tx *sql.Tx) error {
		obs, err := s.getObservationTx(tx, id)
		if err == sql.ErrNoRows {
			return ErrObservationNotFound
		}
		if err != nil {
			return err
		}

		deletedAt := Now()
		if hardDelete {
			if _, err := s.execHook(tx, `DELETE FROM observations WHERE id = ?`, id); err != nil {
				return err
			}
			// ── Phase: memory-conflict-surfacing — C.11 ──────────────────────
			// Orphan any memory_relations rows that reference this observation's
			// sync_id (as source or target). Relations are never cascade-deleted;
			// they become audit history with judgment_status='orphaned'.
			if obs.SyncID != "" {
				if _, err := s.execHook(tx, `
					UPDATE memory_relations
					SET judgment_status = 'orphaned',
					    updated_at      = datetime('now')
					WHERE source_id = ? OR target_id = ?
				`, obs.SyncID, obs.SyncID); err != nil {
					return fmt.Errorf("orphan memory_relations after hard-delete: %w", err)
				}
			}
		} else {
			if _, err := s.execHook(tx,
				`UPDATE observations
				 SET deleted_at = datetime('now'),
				     updated_at = datetime('now')
				 WHERE id = ? AND deleted_at IS NULL`,
				id,
			); err != nil {
				return err
			}
			if err := tx.QueryRow(`SELECT deleted_at FROM observations WHERE id = ?`, id).Scan(&deletedAt); err != nil {
				return err
			}
		}

		return s.enqueueSyncMutationTx(tx, SyncEntityObservation, obs.SyncID, SyncOpDelete, syncObservationPayload{
			SyncID:     obs.SyncID,
			SessionID:  obs.SessionID,
			Project:    obs.Project,
			Deleted:    true,
			DeletedAt:  &deletedAt,
			HardDelete: hardDelete,
		})
	})
}

// ─── Timeline ────────────────────────────────────────────────────────────────
//
// Timeline provides chronological context around a specific observation.
// Given an observation ID, it returns N observations before and M after,
// all within the same session. This is the "progressive disclosure" pattern
// from claude-mem — agents first search, then use timeline to drill into
// the chronological neighborhood of a result.

func (s *SQLiteStore) Timeline(observationID int64, before, after int) (*TimelineResult, error) {
	if before <= 0 {
		before = 5
	}
	if after <= 0 {
		after = 5
	}

	// 1. Get the focus observation
	focus, err := s.GetObservation(observationID)
	if err != nil {
		return nil, fmt.Errorf("timeline: observation #%d not found: %w", observationID, err)
	}

	// 2. Get session info
	session, err := s.GetSession(focus.SessionID)
	if err != nil {
		// Session might be missing for manual-save observations — non-fatal
		session = nil
	}

	// 3. Get observations BEFORE the focus (same session, older, chronological order)
	beforeRows, err := s.queryItHook(s.db, `
		SELECT id, session_id, type, title, content, tool_name, project,
		       scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		FROM observations
		WHERE session_id = ? AND id < ? AND deleted_at IS NULL
		ORDER BY id DESC
		LIMIT ?
	`, focus.SessionID, observationID, before)
	if err != nil {
		return nil, fmt.Errorf("timeline: before query: %w", err)
	}
	defer beforeRows.Close()

	var beforeEntries []TimelineEntry
	for beforeRows.Next() {
		var e TimelineEntry
		if err := beforeRows.Scan(
			&e.ID, &e.SessionID, &e.Type, &e.Title, &e.Content,
			&e.ToolName, &e.Project, &e.Scope, &e.TopicKey, &e.RevisionCount, &e.DuplicateCount, &e.LastSeenAt,
			&e.CreatedAt, &e.UpdatedAt, &e.DeletedAt,
		); err != nil {
			return nil, err
		}
		beforeEntries = append(beforeEntries, e)
	}
	if err := beforeRows.Err(); err != nil {
		return nil, err
	}
	// Reverse to get chronological order (oldest first)
	for i, j := 0, len(beforeEntries)-1; i < j; i, j = i+1, j-1 {
		beforeEntries[i], beforeEntries[j] = beforeEntries[j], beforeEntries[i]
	}

	// 4. Get observations AFTER the focus (same session, newer, chronological order)
	afterRows, err := s.queryItHook(s.db, `
		SELECT id, session_id, type, title, content, tool_name, project,
		       scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		FROM observations
		WHERE session_id = ? AND id > ? AND deleted_at IS NULL
		ORDER BY id ASC
		LIMIT ?
	`, focus.SessionID, observationID, after)
	if err != nil {
		return nil, fmt.Errorf("timeline: after query: %w", err)
	}
	defer afterRows.Close()

	var afterEntries []TimelineEntry
	for afterRows.Next() {
		var e TimelineEntry
		if err := afterRows.Scan(
			&e.ID, &e.SessionID, &e.Type, &e.Title, &e.Content,
			&e.ToolName, &e.Project, &e.Scope, &e.TopicKey, &e.RevisionCount, &e.DuplicateCount, &e.LastSeenAt,
			&e.CreatedAt, &e.UpdatedAt, &e.DeletedAt,
		); err != nil {
			return nil, err
		}
		afterEntries = append(afterEntries, e)
	}
	if err := afterRows.Err(); err != nil {
		return nil, err
	}

	// 5. Count total observations in the session for context
	var totalInRange int
	s.db.QueryRow(
		"SELECT COUNT(*) FROM observations WHERE session_id = ? AND deleted_at IS NULL", focus.SessionID,
	).Scan(&totalInRange)

	return &TimelineResult{
		Focus:        *focus,
		Before:       beforeEntries,
		After:        afterEntries,
		SessionInfo:  session,
		TotalInRange: totalInRange,
	}, nil
}

// ─── Search (FTS5) ───────────────────────────────────────────────────────────

func (s *SQLiteStore) Search(query string, opts SearchOptions) ([]SearchResult, error) {
	// Validate match_mode early so invalid values always error regardless of query shape.
	switch opts.MatchMode {
	case "", "all", "any":
		// valid
	default:
		return nil, fmt.Errorf("invalid match_mode %q: must be \"all\" or \"any\"", opts.MatchMode)
	}

	// Normalize project filter so "Engram" finds records stored as "engram"
	opts.Project, _ = NormalizeProject(opts.Project)

	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > s.cfg.MaxSearchResults {
		limit = s.cfg.MaxSearchResults
	}

	var directResults []SearchResult
	if strings.Contains(query, "/") {
		tkSQL := `
			SELECT ` + observationSelectColumns + `
			FROM observations
			WHERE topic_key = ? AND deleted_at IS NULL
		`
		tkArgs := []any{query}

		if opts.Type != "" {
			tkSQL += " AND type = ?"
			tkArgs = append(tkArgs, opts.Type)
		}
		if opts.Project != "" {
			tkSQL += " AND LOWER(project) = ?"
			tkArgs = append(tkArgs, opts.Project)
		}
		if opts.Scope != "" {
			tkSQL += " AND scope = ?"
			tkArgs = append(tkArgs, normalizeScope(opts.Scope))
		}
		if opts.User != "" {
			tkSQL += " AND created_by = ?"
			tkArgs = append(tkArgs, opts.User)
		}
		if opts.Since != "" {
			if sinceTS := resolveSinceSQLite(opts.Since); sinceTS != "" {
				tkSQL += " AND created_at >= ?"
				tkArgs = append(tkArgs, sinceTS)
			}
		}

		tkSQL += " ORDER BY updated_at DESC LIMIT ?"
		tkArgs = append(tkArgs, limit)

		tkRows, err := s.queryItHook(s.db, tkSQL, tkArgs...)
		if err == nil {
			defer tkRows.Close()
			for tkRows.Next() {
				var sr SearchResult
				if err := tkRows.Scan(
					&sr.ID, &sr.SyncID, &sr.SessionID, &sr.Type, &sr.Title, &sr.Content,
					&sr.ToolName, &sr.Project, &sr.Scope, &sr.TopicKey, &sr.RevisionCount, &sr.DuplicateCount,
					&sr.LastSeenAt, &sr.ReviewAfter, &sr.Pinned, &sr.CreatedAt, &sr.UpdatedAt, &sr.DeletedAt,
				); err != nil {
					break
				}
				sr.Rank = -1000
				directResults = append(directResults, sr)
			}
		}
	}

	// Build FTS5 query: "all" (default) uses AND semantics; "any" uses OR for broader recall.
	var ftsQuery string
	if opts.MatchMode == "any" {
		ftsQuery = sanitizeFTSCandidates(query)
	} else {
		ftsQuery = sanitizeFTS(query)
	}

	sqlQ := `
		SELECT o.id, ifnull(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
		       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.review_after, o.pinned, o.created_at, o.updated_at, o.deleted_at,
		       bm25(observations_fts, 5.0, 1.0, 0.0, 0.0, 0.0, 3.0) as rank
		FROM observations_fts fts
		JOIN observations o ON o.id = fts.rowid
		WHERE observations_fts MATCH ? AND o.deleted_at IS NULL
	`
	args := []any{ftsQuery}

	if opts.Type != "" {
		sqlQ += " AND o.type = ?"
		args = append(args, opts.Type)
	}

	if opts.Project != "" {
		sqlQ += " AND LOWER(o.project) = ?"
		args = append(args, opts.Project)
	}

	if opts.Scope != "" {
		sqlQ += " AND o.scope = ?"
		args = append(args, normalizeScope(opts.Scope))
	}

	if opts.User != "" {
		sqlQ += " AND o.created_by = ?"
		args = append(args, opts.User)
	}

	if opts.Since != "" {
		if sinceTS := resolveSinceSQLite(opts.Since); sinceTS != "" {
			sqlQ += " AND o.created_at >= ?"
			args = append(args, sinceTS)
		}
	}

	sqlQ += " ORDER BY fts.rank LIMIT ?"
	args = append(args, limit)

	rows, err := s.queryItHook(s.db, sqlQ, args...)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	defer rows.Close()

	seen := make(map[int64]bool)
	for _, dr := range directResults {
		seen[dr.ID] = true
	}

	var results []SearchResult
	results = append(results, directResults...)
	for rows.Next() {
		var sr SearchResult
		if err := rows.Scan(
			&sr.ID, &sr.SyncID, &sr.SessionID, &sr.Type, &sr.Title, &sr.Content,
			&sr.ToolName, &sr.Project, &sr.Scope, &sr.TopicKey, &sr.RevisionCount, &sr.DuplicateCount,
			&sr.LastSeenAt, &sr.ReviewAfter, &sr.Pinned, &sr.CreatedAt, &sr.UpdatedAt, &sr.DeletedAt,
			&sr.Rank,
		); err != nil {
			return nil, err
		}
		if !seen[sr.ID] {
			results = append(results, sr)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

// ─── Stats ───────────────────────────────────────────────────────────────────

func (s *SQLiteStore) Stats() (*Stats, error) {
	stats := &Stats{}

	s.db.QueryRow("SELECT COUNT(*) FROM sessions").Scan(&stats.TotalSessions)
	s.db.QueryRow("SELECT COUNT(*) FROM observations WHERE deleted_at IS NULL").Scan(&stats.TotalObservations)
	s.db.QueryRow("SELECT COUNT(*) FROM user_prompts").Scan(&stats.TotalPrompts)

	rows, err := s.queryItHook(s.db, "SELECT project FROM observations WHERE project IS NOT NULL AND deleted_at IS NULL GROUP BY project ORDER BY MAX(created_at) DESC")
	if err != nil {
		return stats, nil
	}
	defer rows.Close()

	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err == nil {
			stats.Projects = append(stats.Projects, p)
		}
	}

	return stats, nil
}

// ─── Project Existence ───────────────────────────────────────────────────────

// ProjectExists returns true if the named project has at least one record in
// any of observations, sessions, prompts, or enrollment tables.
// Uses a single UNION ALL LIMIT 1 query for efficiency (REQ-315).
// The sync_enrolled_projects branch ensures a project enrolled via EnrollProject()
// without any other data is still recognized (JC1).

// Use LOWER(project) = ? so legacy data stored with mixed-case names
// (created before project normalization was enforced on writes) is found
// when queried with the current normalized (lowercase) name. The caller
// is expected to pass an already-normalized name (NormalizeProject result).

// ─── Context Formatting ─────────────────────────────────────────────────────

func (s *SQLiteStore) FormatContext(project, scope string) (string, error) {
	sessions, err := s.RecentSessions(project, 5)
	if err != nil {
		return "", err
	}

	pinned, err := s.PinnedObservations(project, scope)
	if err != nil {
		return "", err
	}

	observations, err := s.recentUnpinnedObservations(project, scope, s.cfg.MaxContextResults)
	if err != nil {
		return "", err
	}

	prompts, err := s.RecentPrompts(project, 10)
	if err != nil {
		return "", err
	}

	if len(sessions) == 0 && len(pinned) == 0 && len(observations) == 0 && len(prompts) == 0 {
		return "", nil
	}

	var b strings.Builder
	b.WriteString("## Memory from Previous Sessions\n\n")

	if len(sessions) > 0 {
		b.WriteString("### Recent Sessions\n")
		for _, sess := range sessions {
			summary := ""
			if sess.Summary != nil {
				summary = fmt.Sprintf(": %s", truncate(*sess.Summary, 200))
			}
			fmt.Fprintf(&b, "- **%s** (%s)%s [%d observations]\n",
				sess.Project, timeutil.FormatLocal(sess.StartedAt), summary, sess.ObservationCount)
		}
		b.WriteString("\n")
	}

	if len(prompts) > 0 {
		b.WriteString("### Recent User Prompts\n")
		for _, p := range prompts {
			fmt.Fprintf(&b, "- %s: %s\n", timeutil.FormatLocal(p.CreatedAt), truncate(p.Content, 200))
		}
		b.WriteString("\n")
	}

	if len(pinned) > 0 {
		b.WriteString("### Pinned\n")
		for _, obs := range pinned {
			fmt.Fprintf(&b, "- [%s] **%s**: %s\n",
				obs.Type, obs.Title, truncate(obs.Content, 300))
		}
		b.WriteString("\n")
	}

	if len(observations) > 0 {
		b.WriteString("### Recent Observations\n")
		for _, obs := range observations {
			fmt.Fprintf(&b, "- [%s] **%s**: %s\n",
				obs.Type, obs.Title, truncate(obs.Content, 300))
		}
		b.WriteString("\n")
	}

	return b.String(), nil
}

// ─── Export / Import ─────────────────────────────────────────────────────────

func (s *SQLiteStore) Export() (*ExportData, error) {
	return s.exportWithProjectScope("")
}

// ExportProject returns an export restricted to records relevant to a single
// normalized project. This avoids full-database exports when only one project
// needs to sync.
func (s *SQLiteStore) ExportProject(project string) (*ExportData, error) {
	normalizedProject, _ := NormalizeProject(project)
	normalizedProject = strings.TrimSpace(normalizedProject)
	if normalizedProject == "" {
		return nil, fmt.Errorf("project is required")
	}
	return s.exportWithProjectScope(normalizedProject)
}

// ExportRelationMutations returns relation upsert mutations for non-orphaned
// relation rows whose source and target observations are available locally.
func (s *SQLiteStore) ExportRelationMutations(project string) ([]SyncMutation, error) {
	normalizedProject, _ := NormalizeProject(project)
	normalizedProject = strings.TrimSpace(normalizedProject)

	query := `
		SELECT r.sync_id, r.source_id, r.target_id, r.relation, r.reason, r.evidence, r.confidence,
		       r.judgment_status, r.marked_by_actor, r.marked_by_kind, r.marked_by_model,
		       r.session_id, coalesce(nullif(src.project, ''), src_s.project, ''), r.created_at, r.updated_at
		FROM memory_relations r
		JOIN observations src ON src.sync_id = r.source_id AND src.deleted_at IS NULL
		JOIN observations tgt ON tgt.sync_id = r.target_id AND tgt.deleted_at IS NULL
		LEFT JOIN sessions src_s ON src_s.id = src.session_id
		LEFT JOIN sessions tgt_s ON tgt_s.id = tgt.session_id
		WHERE r.judgment_status != ?`
	args := []any{JudgmentStatusOrphaned}
	if normalizedProject != "" {
		query += ` AND coalesce(nullif(src.project, ''), src_s.project, '') = ?
			AND coalesce(nullif(tgt.project, ''), tgt_s.project, '') = ?`
		args = append(args, normalizedProject, normalizedProject)
	}
	query += ` ORDER BY r.created_at, r.sync_id`

	rows, err := s.queryItHook(s.db, query, args...)
	if err != nil {
		return nil, fmt.Errorf("export relation mutations: %w", err)
	}
	defer rows.Close()

	mutations := []SyncMutation{}
	for rows.Next() {
		var p syncRelationPayload
		if err := rows.Scan(
			&p.SyncID, &p.SourceID, &p.TargetID, &p.Relation, &p.Reason, &p.Evidence, &p.Confidence,
			&p.JudgmentStatus, &p.MarkedByActor, &p.MarkedByKind, &p.MarkedByModel,
			&p.SessionID, &p.Project, &p.CreatedAt, &p.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("export relation mutations: scan: %w", err)
		}
		payload, err := json.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("export relation mutations: marshal %s: %w", p.SyncID, err)
		}
		mutations = append(mutations, SyncMutation{
			Entity:     SyncEntityRelation,
			EntityKey:  strings.TrimSpace(p.SyncID),
			Op:         SyncOpUpsert,
			Payload:    string(payload),
			Project:    strings.TrimSpace(p.Project),
			OccurredAt: p.UpdatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("export relation mutations: rows: %w", err)
	}
	return mutations, nil
}

func (s *SQLiteStore) exportWithProjectScope(project string) (*ExportData, error) {
	data := &ExportData{
		Version:    "0.1.0",
		ExportedAt: Now(),
	}

	sessionQuery := "SELECT id, project, directory, started_at, ended_at, summary FROM sessions"
	sessionArgs := []any{}
	if project != "" {
		sessionQuery += `
			WHERE project = ?
			   OR id IN (
				SELECT session_id FROM observations
				 WHERE ifnull(project, '') = ?
				    OR (ifnull(project, '') = '' AND session_id IN (SELECT id FROM sessions WHERE project = ?))
				UNION
				SELECT session_id FROM user_prompts
				 WHERE ifnull(project, '') = ?
				    OR (ifnull(project, '') = '' AND session_id IN (SELECT id FROM sessions WHERE project = ?))
			)`
		sessionArgs = append(sessionArgs, project, project, project, project, project)
	}
	sessionQuery += " ORDER BY started_at"

	// Sessions
	rows, err := s.queryItHook(s.db,
		sessionQuery,
		sessionArgs...,
	)
	if err != nil {
		return nil, fmt.Errorf("export sessions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sess Session
		if err := rows.Scan(&sess.ID, &sess.Project, &sess.Directory, &sess.StartedAt, &sess.EndedAt, &sess.Summary); err != nil {
			return nil, err
		}
		data.Sessions = append(data.Sessions, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Observations
	obsQuery := `SELECT ` + observationSelectColumns + `
	 FROM observations`
	obsArgs := []any{}
	if project != "" {
		obsQuery += `
			WHERE ifnull(project, '') = ?
			   OR (ifnull(project, '') = '' AND session_id IN (SELECT id FROM sessions WHERE project = ?))`
		obsArgs = append(obsArgs, project, project)
	}
	obsQuery += " ORDER BY id"
	obsRows, err := s.queryItHook(s.db, obsQuery, obsArgs...)
	if err != nil {
		return nil, fmt.Errorf("export observations: %w", err)
	}
	defer obsRows.Close()
	for obsRows.Next() {
		var o Observation
		if err := scanObservationRowSQLite(obsRows, &o); err != nil {
			return nil, err
		}
		data.Observations = append(data.Observations, o)
	}
	if err := obsRows.Err(); err != nil {
		return nil, err
	}

	// Prompts
	promptQuery := "SELECT id, ifnull(sync_id, '') as sync_id, session_id, content, ifnull(project, '') as project, created_at FROM user_prompts"
	promptArgs := []any{}
	if project != "" {
		promptQuery += `
			WHERE ifnull(project, '') = ?
			   OR (ifnull(project, '') = '' AND session_id IN (SELECT id FROM sessions WHERE project = ?))`
		promptArgs = append(promptArgs, project, project)
	}
	promptQuery += " ORDER BY id"
	promptRows, err := s.queryItHook(s.db, promptQuery, promptArgs...)
	if err != nil {
		return nil, fmt.Errorf("export prompts: %w", err)
	}
	defer promptRows.Close()
	for promptRows.Next() {
		var p Prompt
		if err := promptRows.Scan(&p.ID, &p.SyncID, &p.SessionID, &p.Content, &p.Project, &p.CreatedAt); err != nil {
			return nil, err
		}
		data.Prompts = append(data.Prompts, p)
	}
	if err := promptRows.Err(); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *SQLiteStore) Import(data *ExportData) (*ImportResult, error) {
	tx, err := s.beginTxHook()
	if err != nil {
		return nil, fmt.Errorf("import: begin tx: %w", err)
	}
	defer tx.Rollback()

	result := &ImportResult{}

	// Import sessions (skip duplicates)
	for _, sess := range data.Sessions {
		res, err := s.execHook(tx,
			`INSERT OR IGNORE INTO sessions (id, project, directory, started_at, ended_at, summary)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			sess.ID, sess.Project, sess.Directory, sess.StartedAt, sess.EndedAt, sess.Summary,
		)
		if err != nil {
			return nil, fmt.Errorf("import session %s: %w", sess.ID, err)
		}
		n, _ := res.RowsAffected()
		result.SessionsImported += int(n)
	}

	// Import observations (use new IDs — AUTOINCREMENT, skip duplicate sync IDs)
	for _, obs := range data.Observations {
		syncID := normalizeExistingSyncID(obs.SyncID, "obs")
		res, err := s.execHook(tx,
			`INSERT INTO observations (sync_id, session_id, type, title, content, tool_name, project, scope, topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at, review_after, created_at, updated_at, deleted_at)
			 SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
			 WHERE NOT EXISTS (SELECT 1 FROM observations WHERE sync_id = ?)`,
			syncID,
			obs.SessionID,
			obs.Type,
			obs.Title,
			obs.Content,
			obs.ToolName,
			obs.Project,
			normalizeScope(obs.Scope),
			nullableString(normalizeTopicKey(derefString(obs.TopicKey))),
			hashNormalized(obs.Content),
			maxInt(obs.RevisionCount, 1),
			maxInt(obs.DuplicateCount, 1),
			obs.LastSeenAt,
			obs.ReviewAfter,
			obs.CreatedAt,
			obs.UpdatedAt,
			obs.DeletedAt,
			syncID,
		)
		if err != nil {
			return nil, fmt.Errorf("import observation %d: %w", obs.ID, err)
		}
		n, _ := res.RowsAffected()
		result.ObservationsImported += int(n)
	}

	// Import prompts
	for _, p := range data.Prompts {
		_, err := s.execHook(tx,
			`INSERT INTO user_prompts (sync_id, session_id, content, project, created_at)
			 VALUES (?, ?, ?, ?, ?)`,
			normalizeExistingSyncID(p.SyncID, "prompt"), p.SessionID, p.Content, p.Project, p.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("import prompt %d: %w", p.ID, err)
		}
		result.PromptsImported++
	}

	if err := s.commitHook(tx); err != nil {
		return nil, fmt.Errorf("import: commit: %w", err)
	}

	return result, nil
}

// ─── Sync Chunk Tracking ─────────────────────────────────────────────────────

// GetSyncedChunks returns local-target chunk IDs for backwards compatibility.
func (s *SQLiteStore) GetSyncedChunks() (map[string]bool, error) {
	return s.GetSyncedChunksForTarget(LocalChunkTargetKey)
}

// GetSyncedChunksForTarget returns chunk IDs tracked for a specific sync target.
func (s *SQLiteStore) GetSyncedChunksForTarget(targetKey string) (map[string]bool, error) {
	targetKey = normalizeChunkTargetKey(targetKey)
	rows, err := s.queryItHook(s.db, "SELECT chunk_id FROM sync_chunks WHERE target_key = ?", targetKey)
	if err != nil {
		return nil, fmt.Errorf("get synced chunks: %w", err)
	}
	defer rows.Close()

	chunks := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		chunks[id] = true
	}
	return chunks, rows.Err()
}

// RecordSyncedChunk marks a local-target chunk as imported/exported.
func (s *SQLiteStore) RecordSyncedChunk(chunkID string) error {
	return s.RecordSyncedChunkForTarget(LocalChunkTargetKey, chunkID)
}

// RecordSyncedChunkForTarget marks a chunk as imported/exported for a target.
func (s *SQLiteStore) RecordSyncedChunkForTarget(targetKey, chunkID string) error {
	targetKey = normalizeChunkTargetKey(targetKey)
	_, err := s.execHook(s.db,
		"INSERT OR IGNORE INTO sync_chunks (target_key, chunk_id) VALUES (?, ?)",
		targetKey, chunkID,
	)
	return err
}

// ─── Local Sync State & Mutation Journal ─────────────────────────────────────

func (s *SQLiteStore) GetSyncState(targetKey string) (*SyncState, error) {
	targetKey = normalizeSyncTargetKey(targetKey)
	if err := s.ensureSyncState(targetKey); err != nil {
		return nil, err
	}
	return s.getSyncState(targetKey)
}

func (s *SQLiteStore) ListPendingSyncMutations(targetKey string, limit int) ([]SyncMutation, error) {
	targetKey = normalizeSyncTargetKey(targetKey)
	if limit <= 0 {
		limit = 100
	}
	// Only return mutations for enrolled projects or empty-project (global) mutations.
	// Empty-project mutations always sync regardless of enrollment.
	rows, err := s.queryItHook(s.db, `
		SELECT sm.seq, sm.target_key, sm.entity, sm.entity_key, sm.op, sm.payload, sm.source, sm.project, sm.occurred_at, sm.acked_at
		FROM sync_mutations sm
		LEFT JOIN sync_enrolled_projects sep ON sm.project = sep.project
		WHERE sm.target_key = ? AND sm.acked_at IS NULL
		  AND (sm.project = '' OR sep.project IS NOT NULL)
		ORDER BY sm.seq ASC
		LIMIT ?`, targetKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mutations []SyncMutation
	for rows.Next() {
		var mutation SyncMutation
		if err := rows.Scan(&mutation.Seq, &mutation.TargetKey, &mutation.Entity, &mutation.EntityKey, &mutation.Op, &mutation.Payload, &mutation.Source, &mutation.Project, &mutation.OccurredAt, &mutation.AckedAt); err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	return mutations, rows.Err()
}

func (s *SQLiteStore) ListPendingSyncMutationsAfterSeq(targetKey string, afterSeq int64, limit int) ([]SyncMutation, error) {
	targetKey = normalizeSyncTargetKey(targetKey)
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.queryItHook(s.db, `
		SELECT sm.seq, sm.target_key, sm.entity, sm.entity_key, sm.op, sm.payload, sm.source, sm.project, sm.occurred_at, sm.acked_at
		FROM sync_mutations sm
		LEFT JOIN sync_enrolled_projects sep ON sm.project = sep.project
		WHERE sm.target_key = ? AND sm.acked_at IS NULL
		  AND sm.seq > ?
		  AND (sm.project = '' OR sep.project IS NOT NULL)
		ORDER BY sm.seq ASC
		LIMIT ?`, targetKey, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	mutations := make([]SyncMutation, 0, limit)
	for rows.Next() {
		var mutation SyncMutation
		if err := rows.Scan(&mutation.Seq, &mutation.TargetKey, &mutation.Entity, &mutation.EntityKey, &mutation.Op, &mutation.Payload, &mutation.Source, &mutation.Project, &mutation.OccurredAt, &mutation.AckedAt); err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	return mutations, rows.Err()
}

// SkipAckNonEnrolledMutations acks (marks as skipped) all pending mutations
// that belong to non-enrolled projects, preventing journal bloat. Empty-project
// mutations are never skipped — they always sync regardless of enrollment.
func (s *SQLiteStore) SkipAckNonEnrolledMutations(targetKey string) (int64, error) {
	targetKey = normalizeSyncTargetKey(targetKey)
	res, err := s.execHook(s.db, `
		UPDATE sync_mutations
		SET acked_at = datetime('now')
		WHERE target_key = ?
		  AND acked_at IS NULL
		  AND project != ''
		  AND project NOT IN (SELECT project FROM sync_enrolled_projects)`,
		targetKey,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *SQLiteStore) AckSyncMutations(targetKey string, lastAckedSeq int64) error {
	if lastAckedSeq <= 0 {
		return nil
	}
	targetKey = normalizeSyncTargetKey(targetKey)
	return s.withTx(func(tx *sql.Tx) error {
		affectedProjects := map[string]struct{}{}
		if targetKey == DefaultSyncTargetKey {
			rows, err := s.queryItHook(tx,
				`SELECT DISTINCT ifnull(project, '') FROM sync_mutations
				 WHERE target_key = ? AND seq <= ? AND acked_at IS NULL`,
				targetKey, lastAckedSeq,
			)
			if err != nil {
				return err
			}
			for rows.Next() {
				var project string
				if err := rows.Scan(&project); err != nil {
					_ = rows.Close()
					return err
				}
				project, _ = NormalizeProject(project)
				project = strings.TrimSpace(project)
				if project != "" {
					affectedProjects[project] = struct{}{}
				}
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return err
			}
			_ = rows.Close()
		}

		state, err := s.getSyncStateTx(tx, targetKey)
		if err != nil {
			return err
		}
		if _, err := s.execHook(tx,
			`UPDATE sync_mutations SET acked_at = datetime('now') WHERE target_key = ? AND seq <= ? AND acked_at IS NULL`,
			targetKey, lastAckedSeq,
		); err != nil {
			return err
		}
		acked := state.LastAckedSeq
		if lastAckedSeq > acked {
			acked = lastAckedSeq
		}
		lifecycle := SyncLifecyclePending
		if acked >= state.LastEnqueuedSeq {
			lifecycle = SyncLifecycleHealthy
		}
		if isActivelyDegradedState(state, time.Now().UTC()) {
			lifecycle = SyncLifecycleDegraded
		}
		if lifecycle == SyncLifecycleDegraded {
			_, err = s.execHook(tx,
				`UPDATE sync_state
				 SET last_acked_seq = ?, lifecycle = ?, updated_at = datetime('now')
				 WHERE target_key = ?`,
				acked, lifecycle, targetKey,
			)
		} else {
			_, err = s.execHook(tx,
				`UPDATE sync_state
				 SET last_acked_seq = ?, lifecycle = ?, consecutive_failures = 0, backoff_until = NULL, reason_code = NULL, reason_message = NULL, last_error = NULL, updated_at = datetime('now')
				 WHERE target_key = ?`,
				acked, lifecycle, targetKey,
			)
		}
		if err != nil {
			return err
		}
		if targetKey != DefaultSyncTargetKey {
			return nil
		}
		for project := range affectedProjects {
			if err := s.refreshProjectSyncStateTx(tx, project); err != nil {
				return err
			}
		}
		return nil
	})
}

// AckSyncMutationSeqs acknowledges specific mutation sequence numbers without
// requiring them to be contiguous.
func (s *SQLiteStore) AckSyncMutationSeqs(targetKey string, seqs []int64) error {
	if len(seqs) == 0 {
		return nil
	}
	targetKey = normalizeSyncTargetKey(targetKey)
	return s.withTx(func(tx *sql.Tx) error {
		affectedProjects := map[string]struct{}{}
		if targetKey == DefaultSyncTargetKey {
			for _, seq := range seqs {
				if seq <= 0 {
					continue
				}
				var project string
				err := tx.QueryRow(
					`SELECT ifnull(project, '') FROM sync_mutations WHERE target_key = ? AND seq = ?`,
					targetKey, seq,
				).Scan(&project)
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return err
				}
				project, _ = NormalizeProject(project)
				project = strings.TrimSpace(project)
				if project != "" {
					affectedProjects[project] = struct{}{}
				}
			}
		}

		state, err := s.getSyncStateTx(tx, targetKey)
		if err != nil {
			return err
		}
		maxSeq := state.LastAckedSeq
		for _, seq := range seqs {
			if seq <= 0 {
				continue
			}
			if _, err := s.execHook(tx,
				`UPDATE sync_mutations SET acked_at = datetime('now') WHERE target_key = ? AND seq = ? AND acked_at IS NULL`,
				targetKey, seq,
			); err != nil {
				return err
			}
			if seq > maxSeq {
				maxSeq = seq
			}
		}
		var remaining int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM sync_mutations WHERE target_key = ? AND acked_at IS NULL`, targetKey).Scan(&remaining); err != nil {
			return err
		}
		lifecycle := SyncLifecyclePending
		if remaining == 0 {
			lifecycle = SyncLifecycleHealthy
		}
		if isActivelyDegradedState(state, time.Now().UTC()) {
			lifecycle = SyncLifecycleDegraded
		}
		if lifecycle == SyncLifecycleDegraded {
			_, err = s.execHook(tx,
				`UPDATE sync_state SET last_acked_seq = ?, lifecycle = ?, updated_at = datetime('now') WHERE target_key = ?`,
				maxSeq, lifecycle, targetKey,
			)
		} else {
			_, err = s.execHook(tx,
				`UPDATE sync_state SET last_acked_seq = ?, lifecycle = ?, consecutive_failures = 0, backoff_until = NULL, reason_code = NULL, reason_message = NULL, last_error = NULL, updated_at = datetime('now') WHERE target_key = ?`,
				maxSeq, lifecycle, targetKey,
			)
		}
		if err != nil {
			return err
		}
		if targetKey != DefaultSyncTargetKey {
			return nil
		}
		for project := range affectedProjects {
			if err := s.refreshProjectSyncStateTx(tx, project); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLiteStore) HasPendingSyncMutationsForProject(project string) (bool, error) {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return false, nil
	}

	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM sync_mutations WHERE target_key = ? AND project = ? AND acked_at IS NULL`,
		DefaultSyncTargetKey,
		project,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *SQLiteStore) refreshProjectSyncStateTx(tx *sql.Tx, project string) error {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil
	}
	projectTargetKey := syncTargetKeyForProject(project)
	state, err := s.getSyncStateTx(tx, projectTargetKey)
	if err != nil {
		return err
	}

	var maxAckedSeq int64
	if err := tx.QueryRow(
		`SELECT ifnull(MAX(seq), 0)
		 FROM sync_mutations
		 WHERE target_key = ? AND project = ? AND acked_at IS NOT NULL`,
		DefaultSyncTargetKey,
		project,
	).Scan(&maxAckedSeq); err != nil {
		return err
	}
	if maxAckedSeq < state.LastAckedSeq {
		maxAckedSeq = state.LastAckedSeq
	}

	var maxEnqueuedSeq int64
	if err := tx.QueryRow(
		`SELECT ifnull(MAX(seq), 0)
		 FROM sync_mutations
		 WHERE target_key = ? AND project = ?`,
		DefaultSyncTargetKey,
		project,
	).Scan(&maxEnqueuedSeq); err != nil {
		return err
	}
	if maxEnqueuedSeq < state.LastEnqueuedSeq {
		maxEnqueuedSeq = state.LastEnqueuedSeq
	}

	var pendingCount int
	if err := tx.QueryRow(
		`SELECT COUNT(*)
		 FROM sync_mutations
		 WHERE target_key = ? AND project = ? AND acked_at IS NULL`,
		DefaultSyncTargetKey,
		project,
	).Scan(&pendingCount); err != nil {
		return err
	}

	lifecycle := SyncLifecycleHealthy
	if pendingCount > 0 {
		lifecycle = SyncLifecyclePending
	}
	if isActivelyDegradedState(state, time.Now().UTC()) {
		lifecycle = SyncLifecycleDegraded
	}

	if lifecycle == SyncLifecycleDegraded {
		_, err = s.execHook(tx,
			`UPDATE sync_state
			 SET last_enqueued_seq = ?, last_acked_seq = ?, lifecycle = ?, updated_at = datetime('now')
			 WHERE target_key = ?`,
			maxEnqueuedSeq, maxAckedSeq, lifecycle, projectTargetKey,
		)
	} else {
		_, err = s.execHook(tx,
			`UPDATE sync_state
			 SET last_enqueued_seq = ?, last_acked_seq = ?, lifecycle = ?, consecutive_failures = 0, backoff_until = NULL, reason_code = NULL, reason_message = NULL, last_error = NULL, updated_at = datetime('now')
			 WHERE target_key = ?`,
			maxEnqueuedSeq, maxAckedSeq, lifecycle, projectTargetKey,
		)
	}
	return err
}

func isActivelyDegradedState(state *SyncState, now time.Time) bool {
	if state == nil || state.Lifecycle != SyncLifecycleDegraded {
		return false
	}
	reasonCode := strings.TrimSpace(derefString(state.ReasonCode))
	switch reasonCode {
	case "blocked_unenrolled", "paused", "auth_required", "policy_forbidden", "cloud_config_error":
		return true
	}
	if state.BackoffUntil != nil {
		if backoffUntil, err := time.Parse(time.RFC3339, strings.TrimSpace(*state.BackoffUntil)); err == nil && backoffUntil.After(now) {
			return true
		}
	}
	return false
}

func (s *SQLiteStore) AcquireSyncLease(targetKey, owner string, ttl time.Duration, now time.Time) (bool, error) {
	targetKey = normalizeSyncTargetKey(targetKey)
	if ttl <= 0 {
		ttl = time.Minute
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	var acquired bool
	err := s.withTx(func(tx *sql.Tx) error {
		state, err := s.getSyncStateTx(tx, targetKey)
		if err != nil {
			return err
		}
		if state.LeaseUntil != nil {
			leaseUntil, err := time.Parse(time.RFC3339, *state.LeaseUntil)
			if err == nil && leaseUntil.After(now) && derefString(state.LeaseOwner) != "" && derefString(state.LeaseOwner) != owner {
				acquired = false
				return nil
			}
		}
		leaseUntil := now.Add(ttl).UTC().Format(time.RFC3339)
		_, err = s.execHook(tx,
			`UPDATE sync_state
			 SET lease_owner = ?, lease_until = ?, updated_at = datetime('now')
			 WHERE target_key = ?`,
			owner, leaseUntil, targetKey,
		)
		if err == nil {
			acquired = true
		}
		return err
	})
	return acquired, err
}

func (s *SQLiteStore) ReleaseSyncLease(targetKey, owner string) error {
	targetKey = normalizeSyncTargetKey(targetKey)
	_, err := s.execHook(s.db,
		`UPDATE sync_state
		 SET lease_owner = NULL, lease_until = NULL, updated_at = datetime('now')
		 WHERE target_key = ? AND (lease_owner = ? OR lease_owner IS NULL OR lease_owner = '')`,
		targetKey, owner,
	)
	return err
}

func (s *SQLiteStore) MarkSyncPaused(targetKey, message string) error {
	return s.MarkSyncBlocked(targetKey, "paused", message)
}

func (s *SQLiteStore) MarkSyncAuthRequired(targetKey, message string) error {
	return s.MarkSyncBlocked(targetKey, "auth_required", message)
}

func (s *SQLiteStore) MarkSyncFailure(targetKey, message string, backoffUntil time.Time) error {
	targetKey = normalizeSyncTargetKey(targetKey)
	backoff := backoffUntil.UTC().Format(time.RFC3339)
	return s.withTx(func(tx *sql.Tx) error {
		state, err := s.getSyncStateTx(tx, targetKey)
		if err != nil {
			return err
		}
		_, err = s.execHook(tx,
			`UPDATE sync_state
			 SET lifecycle = ?, consecutive_failures = ?, backoff_until = ?, reason_code = ?, reason_message = ?, last_error = ?, updated_at = datetime('now')
			 WHERE target_key = ?`,
			SyncLifecycleDegraded, state.ConsecutiveFailures+1, backoff, "transport_failed", message, message, targetKey,
		)
		return err
	})
}

func (s *SQLiteStore) MarkSyncHealthy(targetKey string) error {
	targetKey = normalizeSyncTargetKey(targetKey)
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := s.getSyncStateTx(tx, targetKey); err != nil {
			return err
		}
		_, err := s.execHook(tx,
			`UPDATE sync_state
			 SET lifecycle = ?, consecutive_failures = 0, backoff_until = NULL, reason_code = NULL, reason_message = NULL, last_error = NULL, updated_at = datetime('now')
			 WHERE target_key = ?`,
			SyncLifecycleHealthy, targetKey,
		)
		return err
	})
}

func (s *SQLiteStore) MarkSyncPending(targetKey string) error {
	targetKey = normalizeSyncTargetKey(targetKey)
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := s.getSyncStateTx(tx, targetKey); err != nil {
			return err
		}
		_, err := s.execHook(tx,
			`UPDATE sync_state
			 SET lifecycle = ?, consecutive_failures = 0, backoff_until = NULL, reason_code = NULL, reason_message = NULL, last_error = NULL, updated_at = datetime('now')
			 WHERE target_key = ?`,
			SyncLifecyclePending, targetKey,
		)
		return err
	})
}

func (s *SQLiteStore) ApplyPulledMutation(targetKey string, mutation SyncMutation) error {
	targetKey = normalizeSyncTargetKey(targetKey)
	return s.withTx(func(tx *sql.Tx) error {
		state, err := s.getSyncStateTx(tx, targetKey)
		if err != nil {
			return err
		}
		if mutation.Seq <= state.LastPulledSeq {
			return nil
		}

		applyErr := s.applyPulledMutationTx(tx, mutation)
		if applyErr != nil {
			// Phase E: per-entity skip+log policy (design §9).
			// For relation FK misses, write to sync_apply_deferred and ACK the seq
			// so the cursor can advance. All other errors propagate and halt the pull.
			if mutation.Entity == SyncEntityRelation && errors.Is(applyErr, ErrRelationFKMissing) {
				log.Printf("[store] ApplyPulledMutation: relation FK miss seq=%d entity_key=%s — deferring",
					mutation.Seq, mutation.EntityKey)
				if _, deferErr := s.execHook(tx, `
					INSERT INTO sync_apply_deferred
						(sync_id, entity, payload, apply_status, retry_count, first_seen_at)
					VALUES (?, ?, ?, 'deferred', 0, datetime('now'))
					ON CONFLICT(sync_id) DO UPDATE SET
						payload            = excluded.payload,
						last_attempted_at  = datetime('now')
				`, mutation.EntityKey, mutation.Entity, mutation.Payload); deferErr != nil {
					return fmt.Errorf("ApplyPulledMutation: write deferred row: %w", deferErr)
				}
				// Fall through to advance the cursor (ACK the seq).
			} else if mutation.Entity == SyncEntityRelation && errors.Is(applyErr, ErrApplyDead) {
				// Payload is permanently undecodable — write directly as dead and ACK.
				// There is no point retrying; a malformed payload will never become valid.
				log.Printf("[store] ApplyPulledMutation: relation payload dead seq=%d entity_key=%s err=%v — marking dead",
					mutation.Seq, mutation.EntityKey, applyErr)
				if _, deferErr := s.execHook(tx, `
					INSERT INTO sync_apply_deferred
						(sync_id, entity, payload, apply_status, retry_count, first_seen_at)
					VALUES (?, ?, ?, 'dead', 0, datetime('now'))
					ON CONFLICT(sync_id) DO UPDATE SET
						payload           = excluded.payload,
						apply_status      = 'dead',
						last_attempted_at = datetime('now')
				`, mutation.EntityKey, mutation.Entity, mutation.Payload); deferErr != nil {
					return fmt.Errorf("ApplyPulledMutation: write dead row: %w", deferErr)
				}
				// Fall through to advance the cursor (ACK the seq).
			} else {
				return applyErr
			}
		}

		_, err = s.execHook(tx,
			`UPDATE sync_state
			 SET last_pulled_seq = ?, lifecycle = ?, consecutive_failures = 0, backoff_until = NULL, reason_code = NULL, reason_message = NULL, last_error = NULL, updated_at = datetime('now')
			 WHERE target_key = ?`,
			mutation.Seq, SyncLifecycleHealthy, targetKey,
		)
		return err
	})
}

// ApplyPulledChunk atomically applies all mutations contained in a pulled chunk
// and records the chunk as synced in the same transaction. This guarantees
// retry safety: a failed chunk import leaves no partial semantic mutations.
func (s *SQLiteStore) ApplyPulledChunk(targetKey, chunkID string, mutations []SyncMutation) error {
	targetKey = normalizeSyncTargetKey(targetKey)
	chunkTargetKey := normalizeChunkTargetKey(targetKey)
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return fmt.Errorf("chunk id is required")
	}

	return s.withTx(func(tx *sql.Tx) error {
		if _, err := s.getSyncStateTx(tx, targetKey); err != nil {
			return err
		}

		var alreadyImported int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM sync_chunks WHERE target_key = ? AND chunk_id = ?`, chunkTargetKey, chunkID).Scan(&alreadyImported); err != nil {
			return err
		}
		if alreadyImported > 0 {
			return nil
		}

		state, err := s.getSyncStateTx(tx, targetKey)
		if err != nil {
			return err
		}

		seq := state.LastPulledSeq
		for i, mutation := range mutations {
			seq++
			mutation.Seq = seq
			mutation.TargetKey = targetKey
			mutation.Source = SyncSourceRemote
			if err := s.applyPulledMutationTx(tx, mutation); err != nil {
				return fmt.Errorf("apply chunk mutation %d: %w", i, err)
			}
		}

		if _, err := s.execHook(tx,
			`INSERT OR IGNORE INTO sync_chunks (target_key, chunk_id) VALUES (?, ?)`,
			chunkTargetKey, chunkID,
		); err != nil {
			return err
		}

		_, err = s.execHook(tx,
			`UPDATE sync_state
			 SET last_pulled_seq = ?, lifecycle = ?, consecutive_failures = 0, backoff_until = NULL, reason_code = NULL, reason_message = NULL, last_error = NULL, updated_at = datetime('now')
			 WHERE target_key = ?`,
			seq, SyncLifecycleHealthy, targetKey,
		)
		return err
	})
}

// applyPulledMutationTx dispatches a pulled mutation to the appropriate apply handler.

// applyPulledMutationTx dispatches a pulled mutation to the appropriate apply handler.
func (s *SQLiteStore) applyPulledMutationTx(tx *sql.Tx, mutation SyncMutation) error {
	switch mutation.Entity {
	case SyncEntityRelation:
		return s.applyRelationUpsertTx(tx, mutation)
	case SyncEntitySession:
		var payload syncSessionPayload
		if err := decodeSyncPayload([]byte(mutation.Payload), &payload); err != nil {
			return err
		}
		return s.applySessionPayloadTx(tx, payload)
	case SyncEntityObservation:
		var payload syncObservationPayload
		if err := decodeSyncPayload([]byte(mutation.Payload), &payload); err != nil {
			return err
		}
		if mutation.Op == SyncOpDelete {
			return s.applyObservationDeleteTx(tx, payload)
		}
		return s.applyObservationUpsertTx(tx, payload)
	case SyncEntityPrompt:
		var payload syncPromptPayload
		if err := decodeSyncPayload([]byte(mutation.Payload), &payload); err != nil {
			return err
		}
		if mutation.Op == SyncOpDelete {
			return s.applyPromptDeleteTx(tx, payload)
		}
		return s.applyPromptUpsertTx(tx, payload)
	default:
		return fmt.Errorf("unknown sync entity %q", mutation.Entity)
	}
}

func (s *SQLiteStore) GetObservationBySyncID(syncID string) (*Observation, error) {
	row := s.db.QueryRow(
		`SELECT `+observationSelectColumns+`
		 FROM observations WHERE sync_id = ? AND deleted_at IS NULL ORDER BY id DESC LIMIT 1`,
		syncID,
	)
	var o Observation
	if err := scanObservationRowSQLite(row, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

// ─── Project Enrollment for Cloud Sync ───────────────────────────────────────

// EnrollProject registers a project for cloud sync. Idempotent — re-enrolling
// an already-enrolled project is a no-op.
func (s *SQLiteStore) EnrollProject(project string) error {
	project, _ = NormalizeProject(project)
	if project == "" {
		return fmt.Errorf("project name must not be empty")
	}
	return s.withTx(func(tx *sql.Tx) error {
		res, err := s.execHook(tx,
			`INSERT OR IGNORE INTO sync_enrolled_projects (project) VALUES (?)`,
			project,
		)
		if err != nil {
			return err
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			return nil
		}
		return s.backfillProjectSyncMutationsTx(tx, project)
	})
}

// UnenrollProject removes a project from cloud sync enrollment. Idempotent —
// unenrolling a non-enrolled project is a no-op.
func (s *SQLiteStore) UnenrollProject(project string) error {
	project, _ = NormalizeProject(project)
	if project == "" {
		return fmt.Errorf("project name must not be empty")
	}
	_, err := s.execHook(s.db,
		`DELETE FROM sync_enrolled_projects WHERE project = ?`,
		project,
	)
	return err
}

// ListEnrolledProjects returns all projects currently enrolled for cloud sync,
// ordered alphabetically by project name.
func (s *SQLiteStore) ListEnrolledProjects() ([]EnrolledProject, error) {
	rows, err := s.queryItHook(s.db,
		`SELECT project, enrolled_at FROM sync_enrolled_projects ORDER BY project ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []EnrolledProject
	for rows.Next() {
		var ep EnrolledProject
		if err := rows.Scan(&ep.Project, &ep.EnrolledAt); err != nil {
			return nil, err
		}
		projects = append(projects, ep)
	}
	return projects, rows.Err()
}

// IsProjectEnrolled returns true if the given project is enrolled for cloud sync.
func (s *SQLiteStore) IsProjectEnrolled(project string) (bool, error) {
	project, _ = NormalizeProject(project)
	if project == "" {
		return false, nil
	}
	var exists int
	err := s.db.QueryRow(
		`SELECT 1 FROM sync_enrolled_projects WHERE project = ? LIMIT 1`,
		project,
	).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ─── Project Migration ───────────────────────────────────────────────────────

func (s *SQLiteStore) MigrateProject(oldName, newName string) (*MigrateResult, error) {
	if oldName == "" || newName == "" || oldName == newName {
		return &MigrateResult{}, nil
	}

	// Check if old project has any records (short-circuit on first match)
	var exists bool
	err := s.db.QueryRow(
		`SELECT EXISTS(
			SELECT 1 FROM observations WHERE project = ?
			UNION ALL
			SELECT 1 FROM sessions WHERE project = ?
			UNION ALL
			SELECT 1 FROM user_prompts WHERE project = ?
		)`, oldName, oldName, oldName,
	).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("check old project: %w", err)
	}
	if !exists {
		return &MigrateResult{}, nil
	}

	result := &MigrateResult{Migrated: true}

	err = s.withTx(func(tx *sql.Tx) error {
		// FTS triggers handle index updates automatically on UPDATE
		res, err := s.execHook(tx, `UPDATE observations SET project = ? WHERE project = ?`, newName, oldName)
		if err != nil {
			return fmt.Errorf("migrate observations: %w", err)
		}
		result.ObservationsUpdated, _ = res.RowsAffected()

		res, err = s.execHook(tx, `UPDATE sessions SET project = ? WHERE project = ?`, newName, oldName)
		if err != nil {
			return fmt.Errorf("migrate sessions: %w", err)
		}
		result.SessionsUpdated, _ = res.RowsAffected()

		res, err = s.execHook(tx, `UPDATE user_prompts SET project = ? WHERE project = ?`, newName, oldName)
		if err != nil {
			return fmt.Errorf("migrate prompts: %w", err)
		}
		result.PromptsUpdated, _ = res.RowsAffected()

		// Enqueue sync mutations so cloud sync picks up the migrated records.
		// Same pattern used by EnrollProject and MergeProjects.
		return s.backfillProjectSyncMutationsTx(tx, newName)
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// ─── Project Queries ──────────────────────────────────────────────────────────

// ProjectNameCount holds a project name and how many observations it has.
type ProjectNameCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// ProjectExists returns true if the named project has at least one record in
// any of observations, sessions, prompts, or enrollment tables.
// Uses a single UNION ALL LIMIT 1 query for efficiency.
// The sync_enrolled_projects branch ensures a project enrolled via EnrollProject()
// without any other data is still recognized.

// ProjectExists returns true if the named project has at least one record in
// any of observations, sessions, prompts, or enrollment tables.
// Uses a single UNION ALL LIMIT 1 query for efficiency.
// The sync_enrolled_projects branch ensures a project enrolled via EnrollProject()
// without any other data is still recognized.
func (s *SQLiteStore) ProjectExists(name string) (bool, error) {
	const query = `
SELECT 1 FROM (
  SELECT project FROM observations WHERE LOWER(project) = LOWER(?) AND deleted_at IS NULL
  UNION ALL
  SELECT project FROM sessions WHERE LOWER(project) = LOWER(?)
  UNION ALL
  SELECT project FROM user_prompts WHERE LOWER(project) = LOWER(?)
  UNION ALL
  SELECT project FROM sync_enrolled_projects WHERE LOWER(project) = LOWER(?)
) LIMIT 1`
	var dummy int
	err := s.db.QueryRow(query, name, name, name, name).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ListProjectNames returns all distinct project names from observations,
// ordered alphabetically. Used for fuzzy matching and consolidation.
func (s *SQLiteStore) ListProjectNames() ([]string, error) {
	rows, err := s.queryItHook(s.db,
		`SELECT DISTINCT project FROM observations
		 WHERE project IS NOT NULL AND project != '' AND deleted_at IS NULL
		 ORDER BY project`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		results = append(results, name)
	}
	return results, rows.Err()
}

// ListProjectsWithStats returns all projects with aggregated counts.
// Ordered by observation count descending.
func (s *SQLiteStore) ListProjectsWithStats() ([]ProjectDetailStats, error) {
	// Observation counts per project
	obsRows, err := s.queryItHook(s.db,
		`SELECT project, COUNT(*) as cnt
		 FROM observations
		 WHERE project IS NOT NULL AND project != '' AND deleted_at IS NULL
		 GROUP BY project`,
	)
	if err != nil {
		return nil, fmt.Errorf("list projects obs: %w", err)
	}
	defer obsRows.Close()

	statsMap := make(map[string]*ProjectDetailStats)
	for obsRows.Next() {
		var name string
		var cnt int
		if err := obsRows.Scan(&name, &cnt); err != nil {
			return nil, err
		}
		statsMap[name] = &ProjectDetailStats{Name: name, ObservationCount: cnt}
	}
	if err := obsRows.Err(); err != nil {
		return nil, err
	}

	// Session counts + directories per project
	sessRows, err := s.queryItHook(s.db,
		`SELECT project, COUNT(*) as cnt, directory
		 FROM sessions
		 WHERE project IS NOT NULL AND project != ''
		 GROUP BY project, directory`,
	)
	if err != nil {
		return nil, fmt.Errorf("list projects sessions: %w", err)
	}
	defer sessRows.Close()

	type projDir struct {
		count int
		dirs  map[string]bool
	}
	sessData := make(map[string]*projDir)
	for sessRows.Next() {
		var name, dir string
		var cnt int
		if err := sessRows.Scan(&name, &cnt, &dir); err != nil {
			return nil, err
		}
		if sessData[name] == nil {
			sessData[name] = &projDir{dirs: make(map[string]bool)}
		}
		sessData[name].count += cnt
		if dir != "" {
			sessData[name].dirs[dir] = true
		}
	}
	if err := sessRows.Err(); err != nil {
		return nil, err
	}

	for name, sd := range sessData {
		if statsMap[name] == nil {
			statsMap[name] = &ProjectDetailStats{Name: name}
		}
		statsMap[name].SessionCount = sd.count
		for d := range sd.dirs {
			statsMap[name].Directories = append(statsMap[name].Directories, d)
		}
	}

	// Prompt counts per project
	promptRows, err := s.queryItHook(s.db,
		`SELECT project, COUNT(*) as cnt
		 FROM user_prompts
		 WHERE project IS NOT NULL AND project != ''
		 GROUP BY project`,
	)
	if err != nil {
		return nil, fmt.Errorf("list projects prompts: %w", err)
	}
	defer promptRows.Close()

	for promptRows.Next() {
		var name string
		var cnt int
		if err := promptRows.Scan(&name, &cnt); err != nil {
			return nil, err
		}
		if statsMap[name] == nil {
			statsMap[name] = &ProjectDetailStats{Name: name}
		}
		statsMap[name].PromptCount = cnt
	}
	if err := promptRows.Err(); err != nil {
		return nil, err
	}

	// Deprecated status per project
	deprRows, err := s.queryItHook(s.db,
		`SELECT project, deprecated FROM project_metadata WHERE deprecated = 1`,
	)
	if err == nil {
		defer deprRows.Close()
		for deprRows.Next() {
			var name string
			var deprecatedInt int
			if scanErr := deprRows.Scan(&name, &deprecatedInt); scanErr == nil {
				if statsMap[name] != nil {
					statsMap[name].Deprecated = deprecatedInt != 0
				}
			}
		}
	}

	// Convert to slice, sorted by observation count descending
	results := make([]ProjectDetailStats, 0, len(statsMap))
	for _, ps := range statsMap {
		results = append(results, *ps)
	}
	// Simple insertion sort — project lists are small
	for i := 1; i < len(results); i++ {
		for j := i; j > 0 && results[j].ObservationCount > results[j-1].ObservationCount; j-- {
			results[j], results[j-1] = results[j-1], results[j]
		}
	}

	return results, nil
}

// CountObservationsForProject returns the number of non-deleted observations
// for the given project name. Used by handleSave for the similar-project
// warning instead of the heavier ListProjectsWithStats.
func (s *SQLiteStore) CountObservationsForProject(name string) (int, error) {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM observations WHERE project = ? AND deleted_at IS NULL`,
		name,
	).Scan(&count)
	return count, err
}

// MergeResult summarizes the result of merging multiple project name variants
// into a single canonical project name.

// MergeProjects migrates all records from each source project name into the
// canonical name. Sources that equal the canonical (after normalization) or
// have no records are silently skipped — the operation is idempotent.
// All updates are performed inside a single transaction for atomicity.
func (s *SQLiteStore) MergeProjects(sources []string, canonical string) (*MergeResult, error) {
	canonical, _ = NormalizeProject(canonical)
	if canonical == "" {
		return nil, fmt.Errorf("canonical project name must not be empty")
	}

	result := &MergeResult{Canonical: canonical}

	err := s.withTx(func(tx *sql.Tx) error {
		seenSources := make(map[string]struct{})
		for _, srcInput := range sources {
			srcNormalized, _ := NormalizeProject(srcInput)
			if srcNormalized == "" || srcNormalized == canonical {
				continue
			}
			if _, seen := seenSources[srcNormalized]; seen {
				continue
			}
			seenSources[srcNormalized] = struct{}{}

			sourceVariants := projectMergeSourceVariants(srcInput, srcNormalized, canonical)
			if len(sourceVariants) == 0 {
				continue
			}

			placeholders := sqlPlaceholders(len(sourceVariants))
			args := make([]any, 0, len(sourceVariants)+1)
			args = append(args, canonical)
			for _, variant := range sourceVariants {
				args = append(args, variant)
			}

			res, err := s.execHook(tx, `UPDATE observations SET project = ? WHERE project IN (`+placeholders+`)`, args...)
			if err != nil {
				return fmt.Errorf("merge observations %q → %q: %w", srcNormalized, canonical, err)
			}
			n, _ := res.RowsAffected()
			result.ObservationsUpdated += n

			res, err = s.execHook(tx, `UPDATE sessions SET project = ? WHERE project IN (`+placeholders+`)`, args...)
			if err != nil {
				return fmt.Errorf("merge sessions %q → %q: %w", srcNormalized, canonical, err)
			}
			n, _ = res.RowsAffected()
			result.SessionsUpdated += n

			res, err = s.execHook(tx, `UPDATE user_prompts SET project = ? WHERE project IN (`+placeholders+`)`, args...)
			if err != nil {
				return fmt.Errorf("merge prompts %q → %q: %w", srcNormalized, canonical, err)
			}
			n, _ = res.RowsAffected()
			result.PromptsUpdated += n

			result.SourcesMerged = append(result.SourcesMerged, srcNormalized)
		}
		// Enqueue sync mutations so cloud sync picks up the merged records.
		// Same pattern used by EnrollProject.
		return s.backfillProjectSyncMutationsTx(tx, canonical)
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// sqlPlaceholders returns a comma-separated list of parameter markers only.
// Values are still passed separately through query arguments; no user data is
// interpolated into SQL here.
func sqlPlaceholders(count int) string {
	return strings.TrimRight(strings.Repeat("?,", count), ",")
}

func projectMergeSourceVariants(rawSource, normalizedSource, canonical string) []string {
	seen := make(map[string]struct{})
	variants := make([]string, 0, 5)
	// Match both the historical raw project name and its normalized form so
	// legacy rows are migrated without reintroducing canonical-source churn.
	candidates := []string{strings.TrimSpace(rawSource), normalizedSource}
	parts := strings.FieldsFunc(normalizedSource, func(r rune) bool {
		return r == ' ' || r == '-' || r == '_'
	})
	if len(parts) > 1 {
		for _, sep := range []string{" ", "-", "_"} {
			candidates = append(candidates, strings.Join(parts, sep))
		}
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || candidate == canonical {
			continue
		}
		candidateNormalized, _ := NormalizeProject(candidate)
		if candidateNormalized == canonical {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		variants = append(variants, candidate)
	}
	return variants
}

// ─── Project Pruning ─────────────────────────────────────────────────────────

// PruneResult holds the outcome of pruning a single project.

// PruneProject removes all sessions and prompts for a project that has zero
// (non-deleted) observations. Returns an error if the project still has
// observations — the caller must verify first.
func (s *SQLiteStore) PruneProject(project string) (*PruneResult, error) {
	if project == "" {
		return nil, fmt.Errorf("project name must not be empty")
	}

	// Safety check: refuse to prune if observations exist.
	count, err := s.CountObservationsForProject(project)
	if err != nil {
		return nil, fmt.Errorf("count observations: %w", err)
	}
	if count > 0 {
		return nil, fmt.Errorf("project %q still has %d observations — cannot prune", project, count)
	}

	result := &PruneResult{Project: project}

	err = s.withTx(func(tx *sql.Tx) error {
		res, err := s.execHook(tx, `DELETE FROM user_prompts WHERE project = ?`, project)
		if err != nil {
			return fmt.Errorf("prune prompts: %w", err)
		}
		result.PromptsDeleted, _ = res.RowsAffected()

		res, err = s.execHook(tx, `DELETE FROM sessions WHERE project = ?`, project)
		if err != nil {
			return fmt.Errorf("prune sessions: %w", err)
		}
		result.SessionsDeleted, _ = res.RowsAffected()

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// ─── Delete Project ───────────────────────────────────────────────────────────

// DeleteProjectResult summarises a cascade project deletion.
type DeleteProjectResult struct {
	Project             string `json:"project"`
	ObservationsDeleted int64  `json:"observations_deleted"`
	PromptsDeleted      int64  `json:"prompts_deleted"`
	SessionsDeleted     int64  `json:"sessions_deleted"`
	HardDelete          bool   `json:"hard_delete"`
}

// DeleteProject removes all data associated with a project in a single
// transaction.
//
// When hardDelete is true: observation rows are permanently removed, prompts
// are hard-deleted, and sessions are hard-deleted. memory_relations that
// reference any removed observation are marked orphaned (audit history).
//
// When hardDelete is false: observations are soft-deleted (deleted_at set),
// and prompts are hard-deleted. Sessions are NOT removed in this path because
// observations.session_id is a NOT NULL FK to sessions — removing sessions
// while soft-deleted observation rows still reference them would violate the FK
// constraint. The session rows remain and can be cleaned up with
// engram delete session <id> once the observations are purged.
//
// Returns ErrProjectNotFound when no sessions or observations exist for the
// given project name.
func (s *SQLiteStore) DeleteProject(project string, hardDelete bool) (*DeleteProjectResult, error) {
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, fmt.Errorf("project name must not be empty")
	}

	result := &DeleteProjectResult{Project: project, HardDelete: hardDelete}

	err := s.withTx(func(tx *sql.Tx) error {
		// Existence check: at least one session or observation must exist.
		var sessionCount int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM sessions WHERE project = ?`, project).Scan(&sessionCount); err != nil {
			return fmt.Errorf("delete project: count sessions: %w", err)
		}
		var obsCount int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM observations WHERE project = ?`, project).Scan(&obsCount); err != nil {
			return fmt.Errorf("delete project: count observations: %w", err)
		}
		if sessionCount == 0 && obsCount == 0 {
			return fmt.Errorf("%w: %q", ErrProjectNotFound, project)
		}

		// 1. Delete/soft-delete observations.
		if hardDelete {
			// Orphan memory_relations rows that reference any observation in this project.
			if _, err := s.execHook(tx, `
				UPDATE memory_relations
				SET judgment_status = 'orphaned',
				    updated_at      = datetime('now')
				WHERE source_id IN (SELECT sync_id FROM observations WHERE project = ?)
				   OR target_id IN (SELECT sync_id FROM observations WHERE project = ?)
			`, project, project); err != nil {
				return fmt.Errorf("delete project: orphan relations: %w", err)
			}
			res, err := s.execHook(tx, `DELETE FROM observations WHERE project = ?`, project)
			if err != nil {
				return fmt.Errorf("delete project: hard-delete observations: %w", err)
			}
			result.ObservationsDeleted, _ = res.RowsAffected()
		} else {
			res, err := s.execHook(tx, `
				UPDATE observations
				SET deleted_at = datetime('now'),
				    updated_at = datetime('now')
				WHERE project = ? AND deleted_at IS NULL
			`, project)
			if err != nil {
				return fmt.Errorf("delete project: soft-delete observations: %w", err)
			}
			result.ObservationsDeleted, _ = res.RowsAffected()
		}

		// 2. Delete prompts for the project (no soft-delete mechanism exists).
		res, err := s.execHook(tx, `DELETE FROM user_prompts WHERE project = ?`, project)
		if err != nil {
			return fmt.Errorf("delete project: delete prompts: %w", err)
		}
		result.PromptsDeleted, _ = res.RowsAffected()

		// 3. Delete sessions — only when hard-deleting, because observation rows
		//    reference sessions via a NOT NULL FK and soft-deleted rows are still
		//    present in the table.
		if hardDelete {
			res, err = s.execHook(tx, `DELETE FROM sessions WHERE project = ?`, project)
			if err != nil {
				return fmt.Errorf("delete project: delete sessions: %w", err)
			}
			result.SessionsDeleted, _ = res.RowsAffected()
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func (s *SQLiteStore) withTx(fn func(tx *sql.Tx) error) error {
	return withSQLiteWriteRetry(func() error {
		tx, err := s.beginTxHook()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := fn(tx); err != nil {
			return err
		}
		return s.commitHook(tx)
	})
}

func withSQLiteWriteRetry(fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= len(sqliteWriteRetryBackoffs); attempt++ {
		if err := fn(); err != nil {
			lastErr = err
			if !isRetryableSQLiteLockError(err) || attempt == len(sqliteWriteRetryBackoffs) {
				return err
			}
			time.Sleep(sqliteWriteRetryBackoffs[attempt])
			continue
		}
		return nil
	}
	return lastErr
}

func isRetryableSQLiteLockError(err error) bool {
	if err == nil {
		return false
	}
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		primaryCode := sqliteErr.Code() & 0xff
		return primaryCode == sqlitePrimaryBusy || primaryCode == sqlitePrimaryLocked
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "database is busy") || strings.Contains(msg, "sqlite_busy") || strings.Contains(msg, "sqlite_locked")
}

func (s *SQLiteStore) createSessionTx(tx *sql.Tx, id, project, directory string) error {
	_, err := s.execHook(tx,
		`INSERT INTO sessions (id, project, directory) VALUES (?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   project   = CASE WHEN sessions.project = '' THEN excluded.project ELSE sessions.project END,
		   directory = CASE WHEN sessions.directory = '' THEN excluded.directory ELSE sessions.directory END`,
		id, project, directory,
	)
	return err
}

func (s *SQLiteStore) ensureSyncState(targetKey string) error {
	_, err := s.execHook(s.db,
		`INSERT OR IGNORE INTO sync_state (target_key, lifecycle, updated_at) VALUES (?, ?, datetime('now'))`,
		targetKey, SyncLifecycleIdle,
	)
	return err
}

func (s *SQLiteStore) getSyncState(targetKey string) (*SyncState, error) {
	row := s.db.QueryRow(`
		SELECT target_key, lifecycle, last_enqueued_seq, last_acked_seq, last_pulled_seq,
		       consecutive_failures, backoff_until, lease_owner, lease_until, reason_code, reason_message, last_error, updated_at
		FROM sync_state WHERE target_key = ?`, targetKey)
	var state SyncState
	if err := row.Scan(&state.TargetKey, &state.Lifecycle, &state.LastEnqueuedSeq, &state.LastAckedSeq, &state.LastPulledSeq, &state.ConsecutiveFailures, &state.BackoffUntil, &state.LeaseOwner, &state.LeaseUntil, &state.ReasonCode, &state.ReasonMessage, &state.LastError, &state.UpdatedAt); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *SQLiteStore) getSyncStateTx(tx *sql.Tx, targetKey string) (*SyncState, error) {
	if _, err := s.execHook(tx,
		`INSERT OR IGNORE INTO sync_state (target_key, lifecycle, updated_at) VALUES (?, ?, datetime('now'))`,
		targetKey, SyncLifecycleIdle,
	); err != nil {
		return nil, err
	}
	row := tx.QueryRow(`
		SELECT target_key, lifecycle, last_enqueued_seq, last_acked_seq, last_pulled_seq,
		       consecutive_failures, backoff_until, lease_owner, lease_until, reason_code, reason_message, last_error, updated_at
		FROM sync_state WHERE target_key = ?`, targetKey)
	var state SyncState
	if err := row.Scan(&state.TargetKey, &state.Lifecycle, &state.LastEnqueuedSeq, &state.LastAckedSeq, &state.LastPulledSeq, &state.ConsecutiveFailures, &state.BackoffUntil, &state.LeaseOwner, &state.LeaseUntil, &state.ReasonCode, &state.ReasonMessage, &state.LastError, &state.UpdatedAt); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *SQLiteStore) backfillProjectSyncMutationsTx(tx *sql.Tx, project string) error {
	if err := s.backfillSessionSyncMutationsTx(tx, project); err != nil {
		return err
	}
	if err := s.backfillObservationSyncMutationsTx(tx, project); err != nil {
		return err
	}
	if err := s.backfillPromptSyncMutationsTx(tx, project); err != nil {
		return err
	}
	return s.backfillRelationSyncMutationsTx(tx, project)
}

// projectNeedsBackfill returns true when a project has any sessions, live observations,
// or prompts that are missing a corresponding sync_mutation row.
// It runs three lightweight COUNT queries — no cursor is held open.
func (s *SQLiteStore) projectNeedsBackfill(project string) (bool, error) {
	type countQuery struct {
		q    string
		args []any
	}
	queries := []countQuery{
		{
			q: `SELECT COUNT(*) FROM sessions
			    WHERE project = ?
			      AND NOT EXISTS (
			        SELECT 1 FROM sync_mutations sm
			        WHERE sm.target_key = ? AND sm.entity = ? AND sm.entity_key = sessions.id AND sm.source = ?
			      )`,
			args: []any{project, DefaultSyncTargetKey, SyncEntitySession, SyncSourceLocal},
		},
		{
			q: `SELECT COUNT(*) FROM observations o
			    LEFT JOIN sessions s ON s.id = o.session_id
			    WHERE (ifnull(o.project,'') = ? OR (ifnull(o.project,'') = '' AND ifnull(s.project,'') = ?))
			      AND o.deleted_at IS NULL
			      AND NOT EXISTS (
			        SELECT 1 FROM sync_mutations sm
			        WHERE sm.target_key = ? AND sm.entity = ? AND sm.entity_key = o.sync_id AND sm.source = ?
			      )`,
			args: []any{project, project, DefaultSyncTargetKey, SyncEntityObservation, SyncSourceLocal},
		},
		{
			q: `SELECT COUNT(*) FROM user_prompts p
			    LEFT JOIN sessions s ON s.id = p.session_id
			    WHERE (ifnull(p.project,'') = ? OR (ifnull(p.project,'') = '' AND ifnull(s.project,'') = ?))
			      AND NOT EXISTS (
			        SELECT 1 FROM sync_mutations sm
			        WHERE sm.target_key = ? AND sm.entity = ? AND sm.entity_key = p.sync_id AND sm.source = ?
			      )`,
			args: []any{project, project, DefaultSyncTargetKey, SyncEntityPrompt, SyncSourceLocal},
		},
		{
			// Count only fully-judged relations (not orphaned, not pending, with
			// marked_by_actor/kind populated) whose source and target observations
			// are locally available and that have no local upsert sync_mutations row.
			// Mirrors the SELECT in backfillRelationSyncMutationsTx exactly — any
			// divergence causes the fast-path skip to desync from the write path.
			// Pending/unmarked rows lack marked_by_* and would be rejected by cloud
			// validation (HTTP 400), so we exclude them from both the count and the
			// backfill to avoid polluting the sync journal with undeliverable mutations.
			q: `SELECT COUNT(*)
			    FROM memory_relations r
			    JOIN observations src ON src.sync_id = r.source_id AND src.deleted_at IS NULL
			    JOIN observations tgt ON tgt.sync_id = r.target_id AND tgt.deleted_at IS NULL
			    LEFT JOIN sessions src_s ON src_s.id = src.session_id
			    WHERE r.judgment_status NOT IN (?, ?)
			      AND ifnull(r.marked_by_actor, '') != ''
			      AND ifnull(r.marked_by_kind, '') != ''
			      AND coalesce(nullif(src.project, ''), src_s.project, '') = ?
			      AND NOT EXISTS (
			        SELECT 1 FROM sync_mutations sm
			        WHERE sm.target_key = ? AND sm.entity = ? AND sm.entity_key = r.sync_id AND sm.source = ?
			      )`,
			args: []any{JudgmentStatusOrphaned, JudgmentStatusPending, project, DefaultSyncTargetKey, SyncEntityRelation, SyncSourceLocal},
		},
	}
	for _, cq := range queries {
		var n int
		if err := s.db.QueryRow(cq.q, cq.args...).Scan(&n); err != nil {
			return false, err
		}
		if n > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s *SQLiteStore) repairEnrolledProjectSyncMutations() error {
	// Collect enrolled projects outside a transaction so we avoid holding a read
	// cursor open while we later write inside backfillProjectSyncMutationsTx.
	rows, err := s.db.Query(`SELECT project FROM sync_enrolled_projects ORDER BY project ASC`)
	if err != nil {
		return err
	}
	var projects []string
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return closeRowsWithError(rows, err)
		}
		projects = append(projects, project)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, project := range projects {
		// Fast path: if the project is already fully backfilled, skip the write tx entirely.
		needs, err := s.projectNeedsBackfill(project)
		if err != nil {
			return err
		}
		if !needs {
			continue
		}
		if err := s.withTx(func(tx *sql.Tx) error {
			return s.backfillProjectSyncMutationsTx(tx, project)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) backfillSessionSyncMutationsTx(tx *sql.Tx, project string) error {
	rows, err := s.queryItHook(tx, `
		SELECT id, project, directory, started_at, ended_at, summary
		FROM sessions
		WHERE project = ?
		  AND NOT EXISTS (
			SELECT 1
			FROM sync_mutations sm
			WHERE sm.target_key = ?
			  AND sm.entity = ?
			  AND sm.entity_key = sessions.id
			  AND sm.source = ?
		  )
		ORDER BY started_at ASC, id ASC`,
		project, DefaultSyncTargetKey, SyncEntitySession, SyncSourceLocal,
	)
	if err != nil {
		return err
	}

	// Phase 1: collect all missing sessions into memory before any INSERT.
	// Keeping the cursor open while inserting into sync_mutations causes SQLite
	// to re-evaluate the NOT EXISTS subquery against the in-progress write set,
	// which can produce an O(N*M) busy loop on large stores.
	var pending []syncSessionPayload
	for rows.Next() {
		var payload syncSessionPayload
		if err := rows.Scan(&payload.ID, &payload.Project, &payload.Directory, &payload.StartedAt, &payload.EndedAt, &payload.Summary); err != nil {
			return closeRowsWithError(rows, err)
		}
		pending = append(pending, payload)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Phase 2: insert now that the read cursor is closed.
	for _, payload := range pending {
		if err := s.enqueueSyncMutationTx(tx, SyncEntitySession, payload.ID, SyncOpUpsert, payload); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) backfillObservationSyncMutationsTx(tx *sql.Tx, project string) error {
	// ── Live observations ─────────────────────────────────────────────────────
	rows, err := s.queryItHook(tx, `
		SELECT o.sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project, o.scope, o.topic_key,
		       o.revision_count, o.duplicate_count, o.last_seen_at, o.created_at, o.updated_at
		FROM observations o
		LEFT JOIN sessions s ON s.id = o.session_id
		WHERE (
			ifnull(o.project, '') = ?
			OR (ifnull(o.project, '') = '' AND ifnull(s.project, '') = ?)
		)
		  AND deleted_at IS NULL
		  AND NOT EXISTS (
			SELECT 1
			FROM sync_mutations sm
			WHERE sm.target_key = ?
			  AND sm.entity = ?
			  AND sm.entity_key = o.sync_id
			  AND sm.source = ?
		  )
		ORDER BY o.id ASC`,
		project, project, DefaultSyncTargetKey, SyncEntityObservation, SyncSourceLocal,
	)
	if err != nil {
		return err
	}

	// Phase 1: collect live observations before any INSERT.
	var pending []syncObservationPayload
	for rows.Next() {
		var payload syncObservationPayload
		if err := rows.Scan(
			&payload.SyncID,
			&payload.SessionID,
			&payload.Type,
			&payload.Title,
			&payload.Content,
			&payload.ToolName,
			&payload.Project,
			&payload.Scope,
			&payload.TopicKey,
			&payload.RevisionCount,
			&payload.DuplicateCount,
			&payload.LastSeenAt,
			&payload.CreatedAt,
			&payload.UpdatedAt,
		); err != nil {
			return closeRowsWithError(rows, err)
		}
		pending = append(pending, payload)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Phase 2: insert live observation mutations.
	for _, payload := range pending {
		if err := s.enqueueSyncMutationTx(tx, SyncEntityObservation, payload.SyncID, SyncOpUpsert, payload); err != nil {
			return err
		}
	}

	// ── Deleted observations ──────────────────────────────────────────────────
	deletedRows, err := s.queryItHook(tx, `
		SELECT o.sync_id, o.session_id, o.project, o.deleted_at
		FROM observations o
		LEFT JOIN sessions s ON s.id = o.session_id
		WHERE (
			ifnull(o.project, '') = ?
			OR (ifnull(o.project, '') = '' AND ifnull(s.project, '') = ?)
		)
		  AND o.deleted_at IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1
			FROM sync_mutations sm
			WHERE sm.target_key = ?
			  AND sm.entity = ?
			  AND sm.entity_key = o.sync_id
			  AND sm.op = ?
			  AND sm.source = ?
		  )
		ORDER BY o.id ASC`,
		project, project, DefaultSyncTargetKey, SyncEntityObservation, SyncOpDelete, SyncSourceLocal,
	)
	if err != nil {
		return err
	}

	// Phase 1: collect deleted observations before any INSERT.
	var deletedPending []syncObservationPayload
	for deletedRows.Next() {
		var payload syncObservationPayload
		if err := deletedRows.Scan(&payload.SyncID, &payload.SessionID, &payload.Project, &payload.DeletedAt); err != nil {
			return closeRowsWithError(deletedRows, err)
		}
		payload.Deleted = true
		payload.HardDelete = false
		deletedPending = append(deletedPending, payload)
	}
	if err := deletedRows.Close(); err != nil {
		return err
	}
	if err := deletedRows.Err(); err != nil {
		return err
	}

	// Phase 2: insert deleted observation mutations.
	for _, payload := range deletedPending {
		if err := s.enqueueSyncMutationTx(tx, SyncEntityObservation, payload.SyncID, SyncOpDelete, payload); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) backfillPromptSyncMutationsTx(tx *sql.Tx, project string) error {
	// ── Live prompts ──────────────────────────────────────────────────────────
	rows, err := s.queryItHook(tx, `
		SELECT p.sync_id, p.session_id, p.content, p.project, p.created_at
		FROM user_prompts p
		LEFT JOIN sessions s ON s.id = p.session_id
		WHERE (
			ifnull(p.project, '') = ?
			OR (ifnull(p.project, '') = '' AND ifnull(s.project, '') = ?)
		)
		  AND NOT EXISTS (
			SELECT 1
			FROM sync_mutations sm
			WHERE sm.target_key = ?
			  AND sm.entity = ?
			  AND sm.entity_key = p.sync_id
			  AND sm.source = ?
		  )
		ORDER BY p.id ASC`,
		project, project, DefaultSyncTargetKey, SyncEntityPrompt, SyncSourceLocal,
	)
	if err != nil {
		return err
	}

	// Phase 1: collect live prompts before any INSERT.
	var pending []syncPromptPayload
	for rows.Next() {
		var payload syncPromptPayload
		if err := rows.Scan(&payload.SyncID, &payload.SessionID, &payload.Content, &payload.Project, &payload.CreatedAt); err != nil {
			return closeRowsWithError(rows, err)
		}
		pending = append(pending, payload)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Phase 2: insert live prompt mutations.
	for _, payload := range pending {
		if err := s.enqueueSyncMutationTx(tx, SyncEntityPrompt, payload.SyncID, SyncOpUpsert, payload); err != nil {
			return err
		}
	}

	// ── Tombstoned prompts ────────────────────────────────────────────────────
	tombstoneRows, err := s.queryItHook(tx, `
		SELECT prompt_tombstones.sync_id, prompt_tombstones.session_id, prompt_tombstones.project, prompt_tombstones.deleted_at
		FROM prompt_tombstones
		LEFT JOIN sessions s ON s.id = prompt_tombstones.session_id
		WHERE (
			ifnull(prompt_tombstones.project, '') = ?
			OR (ifnull(prompt_tombstones.project, '') = '' AND ifnull(s.project, '') = ?)
		)
		  AND NOT EXISTS (
			SELECT 1
			FROM sync_mutations sm
			WHERE sm.target_key = ?
			  AND sm.entity = ?
			  AND sm.entity_key = prompt_tombstones.sync_id
			  AND sm.source = ?
			  AND sm.op = ?
		  )
		ORDER BY deleted_at ASC`,
		project, project, DefaultSyncTargetKey, SyncEntityPrompt, SyncSourceLocal, SyncOpDelete,
	)
	if err != nil {
		return err
	}

	// Phase 1: collect tombstones before any INSERT.
	var tombstonePending []syncPromptPayload
	for tombstoneRows.Next() {
		var payload syncPromptPayload
		if err := tombstoneRows.Scan(&payload.SyncID, &payload.SessionID, &payload.Project, &payload.DeletedAt); err != nil {
			return closeRowsWithError(tombstoneRows, err)
		}
		payload.Deleted = true
		payload.HardDelete = true
		tombstonePending = append(tombstonePending, payload)
	}
	if err := tombstoneRows.Close(); err != nil {
		return err
	}
	if err := tombstoneRows.Err(); err != nil {
		return err
	}

	// Phase 2: insert tombstone mutations.
	for _, payload := range tombstonePending {
		if err := s.enqueueSyncMutationTx(tx, SyncEntityPrompt, payload.SyncID, SyncOpDelete, payload); err != nil {
			return err
		}
	}
	return nil
}

// backfillRelationSyncMutationsTx creates sync_mutations rows for non-orphaned
// relations that have no corresponding local sync_mutations row.
//
// This fills the cloud-journal gap described in issue #496: a relation can exist
// in memory_relations with no sync_mutations row and therefore never replicates.
//
// Design mirrors backfillObservationSyncMutationsTx exactly:
//   - Phase 1: collect all missing rows into a slice (close cursor first).
//   - Phase 2: insert, avoiding the SQLite cursor-open-during-write busy loop.
//
// The SELECT mirrors ExportRelationMutations' join/orphan-filter structure
// (join both observations, exclude orphaned status, exclude rows that already
// have a local upsert mutation), but scopes by source-observation project only.
// ExportRelationMutations additionally filters by tgt.project; the backfill
// intentionally omits that filter to avoid skipping cross-project edges where
// only the source belongs to this project.
func (s *SQLiteStore) backfillRelationSyncMutationsTx(tx *sql.Tx, project string) error {
	// Only backfill fully-judged relations: exclude orphaned/pending and any row
	// that is missing marked_by_actor or marked_by_kind.  Cloud validation
	// (chunkcodec + server) hard-rejects mutations without those fields (HTTP 400),
	// so enqueueing them would block the entire sync batch.
	// This predicate must stay identical to the COUNT in projectNeedsBackfill.
	rows, err := s.queryItHook(tx, `
		SELECT r.sync_id, r.source_id, r.target_id, r.relation, r.reason, r.evidence, r.confidence,
		       r.judgment_status, r.marked_by_actor, r.marked_by_kind, r.marked_by_model,
		       r.session_id,
		       coalesce(nullif(src.project, ''), src_s.project, ''),
		       r.created_at, r.updated_at
		FROM memory_relations r
		JOIN observations src ON src.sync_id = r.source_id AND src.deleted_at IS NULL
		JOIN observations tgt ON tgt.sync_id = r.target_id AND tgt.deleted_at IS NULL
		LEFT JOIN sessions src_s ON src_s.id = src.session_id
		WHERE r.judgment_status NOT IN (?, ?)
		  AND ifnull(r.marked_by_actor, '') != ''
		  AND ifnull(r.marked_by_kind, '') != ''
		  AND coalesce(nullif(src.project, ''), src_s.project, '') = ?
		  AND NOT EXISTS (
		    SELECT 1 FROM sync_mutations sm
		    WHERE sm.target_key = ?
		      AND sm.entity = ?
		      AND sm.entity_key = r.sync_id
		      AND sm.source = ?
		  )
		ORDER BY r.created_at ASC, r.sync_id ASC`,
		JudgmentStatusOrphaned, JudgmentStatusPending,
		project,
		DefaultSyncTargetKey, SyncEntityRelation, SyncSourceLocal,
	)
	if err != nil {
		return err
	}

	// Phase 1: collect into memory before any INSERT to avoid cursor-open-during-write.
	var pending []syncRelationPayload
	for rows.Next() {
		var p syncRelationPayload
		if err := rows.Scan(
			&p.SyncID, &p.SourceID, &p.TargetID, &p.Relation, &p.Reason, &p.Evidence, &p.Confidence,
			&p.JudgmentStatus, &p.MarkedByActor, &p.MarkedByKind, &p.MarkedByModel,
			&p.SessionID, &p.Project, &p.CreatedAt, &p.UpdatedAt,
		); err != nil {
			return closeRowsWithError(rows, err)
		}
		pending = append(pending, p)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Phase 2: insert now that the read cursor is closed.
	for _, p := range pending {
		if err := s.enqueueSyncMutationTx(tx, SyncEntityRelation, p.SyncID, SyncOpUpsert, p); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) enqueueSyncMutationTx(tx *sql.Tx, entity, entityKey, op string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	project := extractProjectFromPayload(payload)
	project, _ = NormalizeProject(strings.TrimSpace(project))
	if project == "" {
		sessionID := extractSessionIDFromPayload(payload)
		if sessionID != "" {
			if derived, err := s.resolveSessionProjectTx(tx, sessionID); err != nil {
				return err
			} else {
				project = derived
			}
		}
	}
	if _, err := s.execHook(tx,
		`INSERT OR IGNORE INTO sync_state (target_key, lifecycle, updated_at) VALUES (?, ?, datetime('now'))`,
		DefaultSyncTargetKey, SyncLifecycleIdle,
	); err != nil {
		return err
	}
	res, err := s.execHook(tx,
		`INSERT INTO sync_mutations (target_key, entity, entity_key, op, payload, source, project)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		DefaultSyncTargetKey, entity, entityKey, op, string(encoded), SyncSourceLocal, project,
	)
	if err != nil {
		return err
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return err
	}
	_, err = s.execHook(tx,
		`UPDATE sync_state
		 SET lifecycle = ?, last_enqueued_seq = ?, updated_at = datetime('now')
		 WHERE target_key = ?`,
		SyncLifecyclePending, seq, DefaultSyncTargetKey,
	)
	if err != nil {
		return err
	}
	if project == "" {
		return nil
	}
	projectTargetKey := syncTargetKeyForProject(project)
	if _, err := s.execHook(tx,
		`INSERT OR IGNORE INTO sync_state (target_key, lifecycle, updated_at) VALUES (?, ?, datetime('now'))`,
		projectTargetKey, SyncLifecycleIdle,
	); err != nil {
		return err
	}
	_, err = s.execHook(tx,
		`UPDATE sync_state
		 SET lifecycle = ?, last_enqueued_seq = ?, updated_at = datetime('now')
		 WHERE target_key = ?`,
		SyncLifecyclePending, seq, projectTargetKey,
	)
	return err
}

func syncTargetKeyForProject(project string) string {
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return DefaultSyncTargetKey
	}
	return fmt.Sprintf("%s:%s", DefaultSyncTargetKey, project)
}

func extractSessionIDFromPayload(payload any) string {
	switch p := payload.(type) {
	case syncObservationPayload:
		return strings.TrimSpace(p.SessionID)
	case syncPromptPayload:
		return strings.TrimSpace(p.SessionID)
	default:
		data, err := json.Marshal(payload)
		if err != nil {
			return ""
		}
		var generic struct {
			SessionID string `json:"session_id"`
		}
		if err := json.Unmarshal(data, &generic); err != nil {
			return ""
		}
		return strings.TrimSpace(generic.SessionID)
	}
}

func (s *SQLiteStore) resolveSessionProjectTx(tx *sql.Tx, sessionID string) (string, error) {
	if strings.TrimSpace(sessionID) == "" {
		return "", nil
	}
	var project string
	err := tx.QueryRow(`SELECT ifnull(project, '') FROM sessions WHERE id = ?`, sessionID).Scan(&project)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	project, _ = NormalizeProject(strings.TrimSpace(project))
	return project, nil
}

// applyRelationUpsertTx handles a pulled mutation with entity='relation' and
// op='upsert'. It implements the pull-side behavior for Phase 2:
//
//  1. JSON-decode the payload into syncRelationPayload. Decode errors return
//     ErrApplyDead (non-retryable).
//  2. Verify both source and target observations exist locally by sync_id.
//     If either is missing, return ErrRelationFKMissing. The caller must write
//     the raw mutation to sync_apply_deferred and ACK the seq.
//  3. INSERT INTO memory_relations with ON CONFLICT(sync_id) DO UPDATE
//     (last-write-wins, preserving the original created_at).
//  4. On successful apply, DELETE any pre-existing deferred row for this sync_id
//     so it is not retried unnecessarily.

// Step 1: decode payload.

// Step 1b: required field validation — missing source_id or target_id is not
// a retryable FK miss; it is a permanent payload defect (ErrApplyDead).

// Step 2: FK precondition — both observations must exist locally (by sync_id).

// Step 3: upsert into memory_relations keyed on sync_id (idempotent re-apply).

// Step 4: clean up deferred row if one exists (resolves any prior FK-miss deferral).

// extractProjectFromPayload returns the project string from a sync payload struct.
// It handles both string and *string Project fields across all entity payload types.
// Returns empty string if the payload has no project or project is nil.

// Fallback: marshal to JSON and extract $.project via json.Unmarshal.

func (s *SQLiteStore) getObservationTx(tx *sql.Tx, id int64) (*Observation, error) {
	row := tx.QueryRow(
		`SELECT `+observationSelectColumns+`
		 FROM observations WHERE id = ? AND deleted_at IS NULL`, id,
	)
	var o Observation
	if err := scanObservationRowSQLite(row, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

func (s *SQLiteStore) getObservationBySyncIDTx(tx *sql.Tx, syncID string, includeDeleted bool) (*Observation, error) {
	query := `SELECT ` + observationSelectColumns + `
		 FROM observations WHERE sync_id = ?`
	if !includeDeleted {
		query += ` AND deleted_at IS NULL`
	}
	query += ` ORDER BY id DESC LIMIT 1`
	row := tx.QueryRow(query, syncID)
	var o Observation
	if err := scanObservationRowSQLite(row, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

func (s *SQLiteStore) applySessionPayloadTx(tx *sql.Tx, payload syncSessionPayload) error {
	if isSessionDeletePayload(payload) {
		return s.applySessionDeleteTx(tx, payload)
	}
	_, err := s.execHook(tx,
		`INSERT INTO sessions (id, project, directory, started_at, ended_at, summary)
		 VALUES (?, ?, ?, COALESCE(NULLIF(?, ''), datetime('now')), ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   project = excluded.project,
		   directory = excluded.directory,
		   started_at = COALESCE(NULLIF(excluded.started_at, ''), sessions.started_at),
		   ended_at = COALESCE(excluded.ended_at, sessions.ended_at),
		   summary = COALESCE(excluded.summary, sessions.summary)`,
		payload.ID, payload.Project, payload.Directory, strings.TrimSpace(payload.StartedAt), payload.EndedAt, payload.Summary,
	)
	return err
}

func isSessionDeletePayload(payload syncSessionPayload) bool {
	if payload.Deleted || payload.HardDelete {
		return true
	}
	if payload.DeletedAt == nil {
		return false
	}
	return strings.TrimSpace(*payload.DeletedAt) != ""
}

func (s *SQLiteStore) applySessionDeleteTx(tx *sql.Tx, payload syncSessionPayload) error {
	sessionID := strings.TrimSpace(payload.ID)
	if sessionID == "" {
		return nil
	}
	if _, err := s.execHook(tx, `DELETE FROM user_prompts WHERE session_id = ?`, sessionID); err != nil {
		return err
	}
	_, err := s.execHook(tx, `DELETE FROM sessions WHERE id = ?`, sessionID)
	return err
}

func (s *SQLiteStore) applyObservationUpsertTx(tx *sql.Tx, payload syncObservationPayload) error {
	revisionCount := maxInt(payload.RevisionCount, 1)
	duplicateCount := maxInt(payload.DuplicateCount, 1)
	createdAt := strings.TrimSpace(payload.CreatedAt)
	updatedAt := strings.TrimSpace(payload.UpdatedAt)
	if createdAt == "" {
		createdAt = Now()
	}
	if updatedAt == "" {
		updatedAt = createdAt
	}

	existing, err := s.getObservationBySyncIDTx(tx, payload.SyncID, true)
	if err == sql.ErrNoRows {
		_, err = s.execHook(tx,
			`INSERT INTO observations (sync_id, session_id, type, title, content, tool_name, project, scope, topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
			payload.SyncID,
			payload.SessionID,
			payload.Type,
			payload.Title,
			payload.Content,
			payload.ToolName,
			payload.Project,
			normalizeScope(payload.Scope),
			payload.TopicKey,
			hashNormalized(payload.Content),
			revisionCount,
			duplicateCount,
			payload.LastSeenAt,
			createdAt,
			updatedAt,
		)
		return err
	}
	if err != nil {
		return err
	}

	if payload.RevisionCount <= 0 {
		revisionCount = maxInt(existing.RevisionCount, 1)
	}
	if payload.DuplicateCount <= 0 {
		duplicateCount = maxInt(existing.DuplicateCount, 1)
	}
	if payload.LastSeenAt == nil {
		payload.LastSeenAt = existing.LastSeenAt
	}
	if strings.TrimSpace(payload.CreatedAt) == "" {
		createdAt = existing.CreatedAt
	}
	if strings.TrimSpace(payload.UpdatedAt) == "" {
		updatedAt = existing.UpdatedAt
	}

	_, err = s.execHook(tx,
		`UPDATE observations
		 SET session_id = ?, type = ?, title = ?, content = ?, tool_name = ?, project = ?, scope = ?, topic_key = ?, normalized_hash = ?, revision_count = ?, duplicate_count = ?, last_seen_at = ?, created_at = ?, updated_at = ?, deleted_at = NULL
		 WHERE id = ?`,
		payload.SessionID,
		payload.Type,
		payload.Title,
		payload.Content,
		payload.ToolName,
		payload.Project,
		normalizeScope(payload.Scope),
		payload.TopicKey,
		hashNormalized(payload.Content),
		revisionCount,
		duplicateCount,
		payload.LastSeenAt,
		createdAt,
		updatedAt,
		existing.ID,
	)
	return err
}

func (s *SQLiteStore) applyObservationDeleteTx(tx *sql.Tx, payload syncObservationPayload) error {
	existing, err := s.getObservationBySyncIDTx(tx, payload.SyncID, true)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if payload.HardDelete {
		_, err = s.execHook(tx, `DELETE FROM observations WHERE id = ?`, existing.ID)
		return err
	}
	deletedAt := payload.DeletedAt
	if deletedAt == nil {
		now := Now()
		deletedAt = &now
	}
	_, err = s.execHook(tx,
		`UPDATE observations SET deleted_at = ?, updated_at = datetime('now') WHERE id = ?`,
		deletedAt, existing.ID,
	)
	return err
}

func (s *SQLiteStore) applyPromptUpsertTx(tx *sql.Tx, payload syncPromptPayload) error {
	var tombstoneDeletedAt string
	err := tx.QueryRow(`SELECT deleted_at FROM prompt_tombstones WHERE sync_id = ?`, payload.SyncID).Scan(&tombstoneDeletedAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		if isStalePromptUpsert(payload, tombstoneDeletedAt) {
			return nil
		}
		if _, err := s.execHook(tx, `DELETE FROM prompt_tombstones WHERE sync_id = ?`, payload.SyncID); err != nil {
			return err
		}
	}

	var existingID int64
	err = tx.QueryRow(`SELECT id FROM user_prompts WHERE sync_id = ? ORDER BY id DESC LIMIT 1`, payload.SyncID).Scan(&existingID)
	if err == sql.ErrNoRows {
		if strings.TrimSpace(payload.CreatedAt) == "" {
			_, err = s.execHook(tx,
				`INSERT INTO user_prompts (sync_id, session_id, content, project) VALUES (?, ?, ?, ?)`,
				payload.SyncID, payload.SessionID, payload.Content, payload.Project,
			)
		} else {
			_, err = s.execHook(tx,
				`INSERT INTO user_prompts (sync_id, session_id, content, project, created_at) VALUES (?, ?, ?, ?, ?)`,
				payload.SyncID, payload.SessionID, payload.Content, payload.Project, payload.CreatedAt,
			)
		}
		return err
	}
	if err != nil {
		return err
	}
	_, err = s.execHook(tx,
		`UPDATE user_prompts
		 SET session_id = ?,
		     content = ?,
		     project = ?,
		     created_at = CASE WHEN ? = '' THEN created_at ELSE ? END
		 WHERE id = ?`,
		payload.SessionID, payload.Content, payload.Project, strings.TrimSpace(payload.CreatedAt), payload.CreatedAt, existingID,
	)
	return err
}

func (s *SQLiteStore) applyPromptDeleteTx(tx *sql.Tx, payload syncPromptPayload) error {
	if strings.TrimSpace(payload.SyncID) == "" {
		return nil
	}
	if _, err := s.execHook(tx, `DELETE FROM user_prompts WHERE sync_id = ?`, payload.SyncID); err != nil {
		return err
	}
	deletedAt := payload.DeletedAt
	if deletedAt == nil || strings.TrimSpace(*deletedAt) == "" {
		now := Now()
		deletedAt = &now
	}
	_, err := s.execHook(tx,
		`INSERT INTO prompt_tombstones (sync_id, session_id, project, deleted_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(sync_id) DO UPDATE SET session_id = excluded.session_id, project = excluded.project, deleted_at = excluded.deleted_at`,
		payload.SyncID, payload.SessionID, payload.Project, *deletedAt,
	)
	return err
}

func isStalePromptUpsert(payload syncPromptPayload, tombstoneDeletedAt string) bool {
	upsertTime := normalizeComparableTimestamp(payload.CreatedAt)
	if strings.TrimSpace(upsertTime) == "" {
		return true
	}
	return upsertTime <= normalizeComparableTimestamp(tombstoneDeletedAt)
}

func normalizeComparableTimestamp(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return parsed.UTC().Format("2006-01-02 15:04:05")
	}
	return trimmed
}

func parseObservationTime(value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	formats := []string{"2006-01-02 15:04:05", time.RFC3339, time.RFC3339Nano, "2006-01-02"}
	for _, layout := range formats {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp %q", value)
}

type observationScanner interface {
	Scan(dest ...any) error
}

func (s *SQLiteStore) queryObservations(query string, args ...any) ([]Observation, error) {
	rows, err := s.queryItHook(s.db, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Observation
	for rows.Next() {
		var o Observation
		if err := scanObservationRowSQLite(rows, &o); err != nil {
			return nil, err
		}
		results = append(results, o)
	}
	return results, rows.Err()
}

func (s *SQLiteStore) addColumnIfNotExists(tableName, columnName, definition string) error {
	rows, err := s.queryItHook(s.db, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return err
	}

	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return closeRowsWithError(rows, err)
		}
		if name == columnName {
			rows.Close()
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	_, err = s.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, columnName, definition))
	return err
}

func (s *SQLiteStore) migrateSyncChunksTable() error {
	rows, err := s.queryItHook(s.db, "PRAGMA table_info(sync_chunks)")
	if err != nil {
		return err
	}

	hasTargetKey := false
	targetKeyPK := 0
	chunkIDPK := 0
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return closeRowsWithError(rows, err)
		}
		switch name {
		case "target_key":
			hasTargetKey = true
			targetKeyPK = pk
		case "chunk_id":
			chunkIDPK = pk
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	// Already migrated: composite PK (target_key, chunk_id).
	if hasTargetKey && targetKeyPK == 1 && chunkIDPK == 2 {
		return nil
	}

	tx, err := s.beginTxHook()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := s.execHook(tx, `
		CREATE TABLE IF NOT EXISTS sync_chunks_new (
			target_key  TEXT NOT NULL DEFAULT 'local',
			chunk_id    TEXT NOT NULL,
			imported_at TEXT NOT NULL DEFAULT (datetime('now')),
			PRIMARY KEY (target_key, chunk_id)
		)
	`); err != nil {
		return err
	}

	if hasTargetKey {
		if _, err := s.execHook(tx, `
			INSERT OR IGNORE INTO sync_chunks_new (target_key, chunk_id, imported_at)
			SELECT CASE
				WHEN trim(ifnull(target_key, '')) = '' THEN ?
				ELSE trim(target_key)
			END,
			chunk_id,
			imported_at
			FROM sync_chunks
		`, LocalChunkTargetKey); err != nil {
			return err
		}
	} else {
		if _, err := s.execHook(tx, `
			INSERT OR IGNORE INTO sync_chunks_new (target_key, chunk_id, imported_at)
			SELECT ?, chunk_id, imported_at
			FROM sync_chunks
		`, LocalChunkTargetKey); err != nil {
			return err
		}
	}

	if _, err := s.execHook(tx, `DROP TABLE sync_chunks`); err != nil {
		return err
	}
	if _, err := s.execHook(tx, `ALTER TABLE sync_chunks_new RENAME TO sync_chunks`); err != nil {
		return err
	}

	return s.commitHook(tx)
}

func (s *SQLiteStore) migrateLegacyObservationsTable() error {
	rows, err := s.queryItHook(s.db, "PRAGMA table_info(observations)")
	if err != nil {
		return err
	}

	var hasID bool
	var idIsPrimaryKey bool
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return closeRowsWithError(rows, err)
		}
		if name == "id" {
			hasID = true
			idIsPrimaryKey = pk == 1
			break
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	if !hasID || idIsPrimaryKey {
		return nil
	}

	// Legacy tables predate created_by; ensure it exists before the copy below reads it.
	if err := s.addColumnIfNotExists("observations", "created_by", "TEXT"); err != nil {
		return err
	}

	tx, err := s.beginTxHook()
	if err != nil {
		return fmt.Errorf("migrate legacy observations: begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := s.execHook(tx, `
		CREATE TABLE observations_migrated (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			sync_id    TEXT,
			session_id TEXT    NOT NULL,
			type       TEXT    NOT NULL,
			title      TEXT    NOT NULL,
			content    TEXT    NOT NULL,
			tool_name  TEXT,
			project    TEXT,
			scope      TEXT    NOT NULL DEFAULT 'project',
			topic_key  TEXT,
			normalized_hash TEXT,
			revision_count INTEGER NOT NULL DEFAULT 1,
			duplicate_count INTEGER NOT NULL DEFAULT 1,
			last_seen_at TEXT,
			pinned     BOOLEAN NOT NULL DEFAULT 0,
			created_at TEXT    NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT    NOT NULL DEFAULT (datetime('now')),
			deleted_at TEXT,
			created_by TEXT,
			FOREIGN KEY (session_id) REFERENCES sessions(id)
		);
	`); err != nil {
		return fmt.Errorf("migrate legacy observations: create table: %w", err)
	}

	if _, err := s.execHook(tx, `
		INSERT INTO observations_migrated (
			id, sync_id, session_id, type, title, content, tool_name, project,
			scope, topic_key, normalized_hash, revision_count, duplicate_count,
			last_seen_at, pinned, created_at, updated_at, deleted_at, created_by
		)
		SELECT
			CASE
				WHEN id IS NULL THEN NULL
				WHEN ROW_NUMBER() OVER (PARTITION BY id ORDER BY rowid) = 1 THEN CAST(id AS INTEGER)
				ELSE NULL
			END,
			'obs-' || lower(hex(randomblob(16))),
			session_id,
			COALESCE(NULLIF(type, ''), 'manual'),
			COALESCE(NULLIF(title, ''), 'Untitled observation'),
			COALESCE(content, ''),
			tool_name,
			project,
			CASE WHEN scope IS NULL OR scope = '' THEN 'project' ELSE scope END,
			NULLIF(topic_key, ''),
			normalized_hash,
			CASE WHEN revision_count IS NULL OR revision_count < 1 THEN 1 ELSE revision_count END,
			CASE WHEN duplicate_count IS NULL OR duplicate_count < 1 THEN 1 ELSE duplicate_count END,
			last_seen_at,
			0,
			COALESCE(NULLIF(created_at, ''), datetime('now')),
			COALESCE(NULLIF(updated_at, ''), NULLIF(created_at, ''), datetime('now')),
			deleted_at,
			created_by
		FROM observations
		ORDER BY rowid;
	`); err != nil {
		return fmt.Errorf("migrate legacy observations: copy rows: %w", err)
	}

	if _, err := s.execHook(tx, "DROP TABLE observations"); err != nil {
		return fmt.Errorf("migrate legacy observations: drop old table: %w", err)
	}

	if _, err := s.execHook(tx, "ALTER TABLE observations_migrated RENAME TO observations"); err != nil {
		return fmt.Errorf("migrate legacy observations: rename table: %w", err)
	}

	if _, err := s.execHook(tx, `
		DROP TRIGGER IF EXISTS obs_fts_insert;
		DROP TRIGGER IF EXISTS obs_fts_update;
		DROP TRIGGER IF EXISTS obs_fts_delete;
		DROP TABLE IF EXISTS observations_fts;
		CREATE VIRTUAL TABLE observations_fts USING fts5(
			title,
			content,
			tool_name,
			type,
			project,
			topic_key,
			content='observations',
			content_rowid='id'
		);
		INSERT INTO observations_fts(rowid, title, content, tool_name, type, project, topic_key)
		SELECT id, title, content, tool_name, type, project, topic_key
		FROM observations
		WHERE deleted_at IS NULL;
	`); err != nil {
		return fmt.Errorf("migrate legacy observations: rebuild fts: %w", err)
	}

	if err := s.commitHook(tx); err != nil {
		return fmt.Errorf("migrate legacy observations: commit: %w", err)
	}

	return nil
}

// NormalizeProject applies canonical project name normalization:
// lowercase + trim whitespace + collapse consecutive hyphens/underscores.
// Returns the normalized name and a warning message if the name was changed
// (empty string if no change was needed).
// Exported so MCP and CLI handlers can surface the warning to users.

// Collapse multiple consecutive hyphens

// Collapse multiple consecutive underscores

// SuggestTopicKey generates a stable topic key suggestion from type/title/content.
// It infers a topic family (e.g. architecture/*, bug/*) and then appends
// a normalized segment from title/content for stable cross-session keys.

func dedupeWindowExpression(window time.Duration) string {
	if window <= 0 {
		window = 15 * time.Minute
	}
	minutes := int(window.Minutes())
	if minutes < 1 {
		minutes = 1
	}
	return "-" + strconv.Itoa(minutes) + " minutes"
}

func normalizeChunkTargetKey(targetKey string) string {
	if strings.TrimSpace(targetKey) == "" {
		return LocalChunkTargetKey
	}
	return strings.TrimSpace(strings.ToLower(targetKey))
}

// privateTagRegex matches <private>...</private> tags and their contents.
// Supports multiline and nested content. Case-insensitive.

// stripPrivateTags removes all <private>...</private> content from a string.
// This ensures sensitive information (API keys, passwords, personal data)
// is never persisted to the memory database.

// Clean up multiple consecutive [REDACTED] and excessive whitespace

// sanitizeFTS wraps each word in quotes so FTS5 doesn't choke on special chars.
// "fix auth bug" → `"fix" "auth" "bug"`

// Strip existing quotes to avoid double-quoting

// Double interior double-quotes: FTS5 escapes a literal " inside a
// quoted phrase by doubling it (""). Without this, `hello"world`
// becomes `"hello"world"` — an unterminated string literal that crashes
// the query with "SQL logic error: unterminated string". See #574.

// ─── Passive Capture ─────────────────────────────────────────────────────────

// PassiveCaptureParams holds the input for passive memory capture.

// e.g. "subagent-stop", "session-end"

// PassiveCaptureResult holds the output of passive memory capture.

// Total learnings found in text
// New observations created
// Skipped because already existed

// learningHeaderPattern matches section headers for learnings in both English and Spanish.

// ExtractLearnings parses structured learning items from text.
// It looks for sections like "## Key Learnings:" or "## Aprendizajes Clave:"
// and extracts numbered (1. text) or bullet (- text) items.
// Returns learnings from the LAST matching section (most recent output).

// Process sections in reverse — use first valid one (most recent)

// Cut off at next major section header

// Try numbered items: "1. text" or "1) text"

// Fall back to bullet items: "- text" or "* text"

// cleanMarkdown strips basic markdown formatting and collapses whitespace.

// bold
// inline code
// italic

// PassiveCapture extracts learnings from text and saves them as observations.
// It deduplicates against existing observations using content hash matching.
func (s *SQLiteStore) PassiveCapture(p PassiveCaptureParams) (*PassiveCaptureResult, error) {
	// Normalize project name before storing
	p.Project, _ = NormalizeProject(p.Project)

	result := &PassiveCaptureResult{}

	learnings := ExtractLearnings(p.Content)
	result.Extracted = len(learnings)

	if len(learnings) == 0 {
		return result, nil
	}

	for _, learning := range learnings {
		// Check if this learning already exists (by content hash) within this project
		normHash := hashNormalized(learning)
		var existingID int64
		err := s.db.QueryRow(
			`SELECT id FROM observations
			 WHERE normalized_hash = ?
			   AND ifnull(project, '') = ifnull(?, '')
			   AND deleted_at IS NULL
			 LIMIT 1`,
			normHash, nullableString(p.Project),
		).Scan(&existingID)

		if err == nil {
			// Already exists — skip
			result.Duplicates++
			continue
		}

		// Truncate for title: first 60 chars
		title := learning
		if len(title) > 60 {
			title = title[:60] + "..."
		}

		_, err = s.AddObservation(AddObservationParams{
			SessionID: p.SessionID,
			Type:      "passive",
			Title:     title,
			Content:   learning,
			Project:   p.Project,
			Scope:     "project",
			ToolName:  p.Source,
		})
		if err != nil {
			return result, fmt.Errorf("passive capture save: %w", err)
		}
		result.Saved++
	}

	return result, nil
}

// resolveSinceSQLite converts a human-readable time filter to a UTC timestamp
// string suitable for SQLite comparison.
func resolveSinceSQLite(since string) string {
	const tsFmt = "2006-01-02 15:04:05"
	now := time.Now().UTC()
	switch strings.TrimSpace(strings.ToLower(since)) {
	case "today":
		y, m, d := now.Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format(tsFmt)
	case "yesterday":
		y, m, d := now.AddDate(0, 0, -1).Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format(tsFmt)
	case "week":
		y, m, d := now.AddDate(0, 0, -7).Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format(tsFmt)
	case "month":
		y, m, d := now.AddDate(0, -1, 0).Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format(tsFmt)
	default:
		if t, err := time.Parse("2006-01-02", strings.TrimSpace(since)); err == nil {
			return t.UTC().Format(tsFmt)
		}
		if t, err := time.Parse(tsFmt, strings.TrimSpace(since)); err == nil {
			return t.UTC().Format(tsFmt)
		}
	}
	return ""
}

// ListProjects returns all projects with observation counts, contributor counts,
// and last activity date. SQLite backend.
// When includeDeprecated is false, deprecated projects are excluded.
func (s *SQLiteStore) ListProjects(includeDeprecated bool) ([]ProjectStats, error) {
	includeInt := 0
	if includeDeprecated {
		includeInt = 1
	}
	rows, err := s.queryItHook(s.db, `
		SELECT o.project,
		       COUNT(*) AS observations,
		       COUNT(DISTINCT COALESCE(o.created_by, '')) AS contributors,
		       MAX(o.updated_at) AS last_activity,
		       COALESCE(pm.deprecated, 0) AS deprecated
		FROM observations o
		LEFT JOIN project_metadata pm ON pm.project = o.project
		WHERE o.deleted_at IS NULL
		  AND o.project IS NOT NULL
		  AND o.project != ''
		  AND (? = 1 OR pm.deprecated IS NULL OR pm.deprecated = 0)
		GROUP BY o.project, pm.deprecated
		ORDER BY last_activity DESC`, includeInt)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()

	var results []ProjectStats
	for rows.Next() {
		var ps ProjectStats
		var deprecatedInt int
		if err := rows.Scan(&ps.Project, &ps.Observations, &ps.Contributors, &ps.LastActivity, &deprecatedInt); err != nil {
			return nil, err
		}
		ps.Deprecated = deprecatedInt != 0
		results = append(results, ps)
	}
	return results, rows.Err()
}

// ─── Test-accessor helpers (REQ-009 / Phase G integration tests) ──────────────

// CountRelationSyncMutations returns the number of sync_mutations rows whose
// entity is NOT 'session', 'observation', or 'prompt'. Used by integration
// tests to verify the enrollment gate: an UNENROLLED project must never enqueue
// relation sync mutations (the enqueue in JudgeBySemantic/JudgeRelation is
// guarded by an enrollment check). The test that calls this uses an unenrolled
// store, so the count must remain zero.
//
// Note: relation sync mutations ARE valid for enrolled projects (#313/#379/#383
// enabled cloud relation sync; #496 extends it with backfill). This function
// is not a blanket "relations are local-only" check — it is an enrollment-gate
// regression guard scoped to the unenrolled test context that uses it.
func (s *SQLiteStore) CountRelationSyncMutations() (int, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT count(*)
		FROM sync_mutations
		WHERE entity NOT IN ('session', 'observation', 'prompt')
	`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("CountRelationSyncMutations: %w", err)
	}
	return count, nil
}

// ─── Phase E: sync_apply_deferred helpers ────────────────────────────────────

// ReplayDeferredResult holds counts returned by ReplayDeferred.

// ReplayDeferred retries all rows in sync_apply_deferred with apply_status='deferred'
// (up to 50 per call, ordered by first_seen_at). For each row:
//   - Calls applyPulledMutationTx inside a transaction.
//   - On success: the apply itself deletes the deferred row (applyRelationUpsertTx
//     already includes DELETE FROM sync_apply_deferred on success path).
//   - On ErrRelationFKMissing: increments retry_count; if retry_count reaches 5,
//     marks apply_status='dead'. Otherwise updates last_error + last_attempted_at.
//   - On ErrApplyDead or other decode errors: marks apply_status='dead'.
//
// Dead rows are never retried. Idempotent: calling twice in one cycle does not
// double-retry because successful rows are deleted and failed rows update retry_count
// in place.
//
// Returns counts (retried, succeeded, failed, dead) for caller logging.

// Success: applyRelationUpsertTx already deleted the deferred row.

// Classify the error and update the deferred row.

// Still retryable.

// Dead: either retry cap reached or non-retryable error.

// CountDeferredAndDead returns the count of rows in sync_apply_deferred grouped
// by apply_status. Only 'deferred' and 'dead' statuses are counted; 'applied'
// rows (if any) are not included.

// ─── Phase 3: ListDeferred / GetDeferred ─────────────────────────────────────

// ListDeferred returns rows from sync_apply_deferred with optional status filter
// and pagination. The payload field is decoded to map[string]any; on malformed
// JSON, PayloadValid is false and PayloadRaw is preserved.

// GetDeferred returns a single row from sync_apply_deferred by sync_id.
// Returns an error wrapping "not found" when no row exists (matches FindCandidates style).

// scannable is a common interface for *sql.Row and *sql.Rows.Scan.
type scannable interface {
	Scan(dest ...any) error
}

// scanDeferredRow scans a single sync_apply_deferred row into a DeferredRow.
// The payload is decoded to map[string]any; malformed JSON sets PayloadValid=false.
func scanDeferredRow(row scannable) (DeferredRow, error) {
	var r DeferredRow
	var rawPayload string
	if err := row.Scan(
		&r.SyncID, &r.Entity, &rawPayload, &r.ApplyStatus, &r.RetryCount,
		&r.LastError, &r.LastAttemptedAt, &r.FirstSeenAt,
	); err != nil {
		return r, err
	}
	r.PayloadRaw = rawPayload
	var decoded map[string]any
	if err := json.Unmarshal([]byte(rawPayload), &decoded); err == nil {
		r.Payload = decoded
		r.PayloadValid = true
	} else {
		r.PayloadValid = false
	}
	return r, nil
}

// ListObservationSyncPayloads returns the decoded payloads of all sync_mutations
// rows whose entity = 'observation'. Used by integration tests to assert that
// new observation columns (review_after, expires_at, embedding*) are NOT present
// in the sync wire format in Phase 1 (REQ-009).
func (s *SQLiteStore) ListObservationSyncPayloads() ([]any, error) {
	rows, err := s.db.Query(`
		SELECT payload
		FROM sync_mutations
		WHERE entity = 'observation'
		ORDER BY seq ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("ListObservationSyncPayloads: query: %w", err)
	}
	defer rows.Close()

	var payloads []any
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("ListObservationSyncPayloads: scan: %w", err)
		}
		var p syncObservationPayload
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, fmt.Errorf("ListObservationSyncPayloads: unmarshal: %w", err)
		}
		payloads = append(payloads, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ListObservationSyncPayloads: rows error: %w", err)
	}
	return payloads, nil
}

// DeprecateProject marks a project as deprecated in project_metadata.
// It upserts the row setting deprecated=1, deprecated_at=NOW(), deprecated_by=identity.
func (s *SQLiteStore) DeprecateProject(project, identity string) error {
	if project == "" {
		return fmt.Errorf("project name must not be empty")
	}
	_, err := s.execHook(s.db, `
		INSERT INTO project_metadata (project, deprecated, deprecated_at, deprecated_by, updated_at)
		VALUES (?, 1, datetime('now'), ?, datetime('now'))
		ON CONFLICT (project) DO UPDATE
		  SET deprecated    = 1,
		      deprecated_at = datetime('now'),
		      deprecated_by = ?,
		      updated_at    = datetime('now')`,
		project, identity, identity)
	if err != nil {
		return fmt.Errorf("deprecate project: %w", err)
	}
	return nil
}

// ActivateProject removes the deprecated flag from a project.
// If the row doesn't exist, this is a no-op (project is active by default).
func (s *SQLiteStore) ActivateProject(project string) error {
	if project == "" {
		return fmt.Errorf("project name must not be empty")
	}
	_, err := s.execHook(s.db, `
		UPDATE project_metadata
		SET deprecated    = 0,
		    deprecated_at = NULL,
		    deprecated_by = NULL,
		    updated_at    = datetime('now')
		WHERE project = ?`,
		project)
	if err != nil {
		return fmt.Errorf("activate project: %w", err)
	}
	return nil
}

// IsProjectDeprecated returns true if the project exists in project_metadata with deprecated=1.
func (s *SQLiteStore) IsProjectDeprecated(project string) (bool, error) {
	if project == "" {
		return false, nil
	}
	var deprecatedInt int
	err := s.db.QueryRow(`
		SELECT deprecated FROM project_metadata WHERE project = ?`, project,
	).Scan(&deprecatedInt)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("is project deprecated: %w", err)
	}
	return deprecatedInt != 0, nil
}

// PromoteObservation changes an observation's scope from 'personal' to 'project'.
// It validates that the observation exists, is personal, and is owned by identity.
func (s *SQLiteStore) PromoteObservation(id int64, identity string) error {
	result, err := s.db.Exec(
		`UPDATE observations
		 SET scope = 'project', updated_at = datetime('now'), revision_count = revision_count + 1
		 WHERE id = ? AND scope = 'personal' AND created_by = ? AND deleted_at IS NULL`,
		id, identity,
	)
	if err != nil {
		return fmt.Errorf("promote observation: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("observation not found, not personal scope, or not owned by you")
	}
	return nil
}

// ListContributors returns contributor activity stats from observations and prompts.
// SQLite — created_by is only populated on rows synced from a multi-user backend;
// local-only writes leave it NULL, so this typically returns empty.
func (s *SQLiteStore) ListContributors(project string) ([]ContributorStats, error) {
	obsQuery := `
		SELECT COALESCE(created_by, '') as identity,
		       COUNT(*) AS obs_count,
		       MAX(updated_at) AS last_obs
		FROM observations
		WHERE deleted_at IS NULL
		  AND created_by IS NOT NULL
		  AND created_by != ''`
	obsArgs := []any{}
	if project != "" {
		obsQuery += " AND project = ?"
		obsArgs = append(obsArgs, project)
	}
	obsQuery += " GROUP BY created_by"

	rows, err := s.queryItHook(s.db, obsQuery, obsArgs...)
	if err != nil {
		return nil, fmt.Errorf("list contributors: %w", err)
	}
	defer rows.Close()

	type obsAgg struct {
		obsCount   int
		lastActive string
	}
	obsMap := make(map[string]obsAgg)
	for rows.Next() {
		var identity string
		var count int
		var lastObs string
		if err := rows.Scan(&identity, &count, &lastObs); err != nil {
			return nil, err
		}
		obsMap[identity] = obsAgg{obsCount: count, lastActive: lastObs}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var results []ContributorStats
	for identity, oa := range obsMap {
		results = append(results, ContributorStats{
			Identity:     identity,
			Observations: oa.obsCount,
			LastActive:   oa.lastActive,
		})
	}

	// Sort by last active descending.
	for i := 1; i < len(results); i++ {
		for j := i; j > 0 && results[j].LastActive > results[j-1].LastActive; j-- {
			results[j], results[j-1] = results[j-1], results[j]
		}
	}

	return results, nil
}

// Compile-time assertion that *SQLiteStore satisfies the Store interface.
var _ Store = (*SQLiteStore)(nil)

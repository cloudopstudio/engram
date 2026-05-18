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

	sqlite "modernc.org/sqlite"
)

var openDB = sql.Open

// sqliteConstraintForeignKey is the extended SQLite result code for a foreign-key
// constraint violation (SQLITE_CONSTRAINT_FOREIGNKEY = 787).
// See https://www.sqlite.org/rescode.html#constraint_foreignkey
const sqliteConstraintForeignKey = 787

// ─── Decay constants ─────────────────────────────────────────────────────────
//
// Decay defaults — months added to now() to compute review_after on new inserts.
// expires_at is NULL for all types in Phase 1.
const (
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
			created_at TEXT    NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT    NOT NULL DEFAULT (datetime('now')),
			deleted_at TEXT,
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
				chunk_id    TEXT PRIMARY KEY,
				imported_at TEXT NOT NULL DEFAULT (datetime('now'))
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
		{name: "updated_at", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "deleted_at", definition: "TEXT"},
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
		CREATE INDEX IF NOT EXISTS idx_sync_mutations_target_seq ON sync_mutations(target_key, seq);
		CREATE INDEX IF NOT EXISTS idx_sync_mutations_pending ON sync_mutations(target_key, acked_at, seq);
	`); err != nil {
		return err
	}

	// Project-scoped sync: add project column to sync_mutations and enrollment table.
	if err := s.addColumnIfNotExists("sync_mutations", "project", "TEXT NOT NULL DEFAULT ''"); err != nil {
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
	if _, err := s.execHook(s.db, `UPDATE user_prompts SET sync_id = 'prompt-' || lower(hex(randomblob(16))) WHERE sync_id IS NULL OR sync_id = ''`); err != nil {
		return err
	}
	if _, err := s.execHook(s.db, `INSERT OR IGNORE INTO sync_state (target_key, lifecycle, updated_at) VALUES ('cloud', 'idle', datetime('now'))`); err != nil {
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
		return s.enqueueSyncMutationTx(tx, SyncEntitySession, id, SyncOpUpsert, syncSessionPayload{
			ID:        id,
			Project:   project,
			Directory: directory,
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

		var endedAt string
		var project, directory string
		var storedSummary *string
		if err := tx.QueryRow(
			`SELECT project, directory, ended_at, summary FROM sessions WHERE id = ?`,
			id,
		).Scan(&project, &directory, &endedAt, &storedSummary); err != nil {
			return err
		}

		return s.enqueueSyncMutationTx(tx, SyncEntitySession, id, SyncOpUpsert, syncSessionPayload{
			ID:        id,
			Project:   project,
			Directory: directory,
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
		query += " AND s.project = ?"
		args = append(args, project)
	}

	query += " GROUP BY s.id ORDER BY MAX(COALESCE(o.created_at, s.started_at)) DESC LIMIT ?"
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

	query += " GROUP BY s.id ORDER BY MAX(COALESCE(o.created_at, s.started_at)) DESC LIMIT ?"
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
		SELECT o.id, ifnull(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
		       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.created_at, o.updated_at, o.deleted_at
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

	query += " ORDER BY o.created_at DESC LIMIT ?"
	args = append(args, limit)

	return s.queryObservations(query, args...)
}

// SessionObservations returns all observations for a specific session.
func (s *SQLiteStore) SessionObservations(sessionID string, limit int) ([]Observation, error) {
	if limit <= 0 {
		limit = 200
	}

	query := `
		SELECT id, ifnull(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		       scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
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
		SELECT o.id, ifnull(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
		       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.created_at, o.updated_at, o.deleted_at
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

	query += " ORDER BY o.created_at DESC, o.id DESC LIMIT ?"
	args = append(args, limit)

	return s.queryObservations(query, args...)
}

// ─── User Prompts ────────────────────────────────────────────────────────────

func (s *SQLiteStore) AddPrompt(p AddPromptParams) (int64, error) {
	// Normalize project name before storing
	p.Project, _ = NormalizeProject(p.Project)

	content := stripPrivateTags(p.Content)
	if len(content) > s.cfg.MaxObservationLength {
		content = content[:s.cfg.MaxObservationLength] + "... [truncated]"
	}

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
		return s.enqueueSyncMutationTx(tx, SyncEntityPrompt, syncID, SyncOpUpsert, syncPromptPayload{
			SyncID:    syncID,
			SessionID: p.SessionID,
			Content:   content,
			Project:   nullableString(p.Project),
		})
	})
	if err != nil {
		return 0, err
	}
	return promptID, nil
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
// Note: this delete only removes local rows. It does not enqueue a delete
// sync mutation, but any previously enqueued mutations for the session or its
// prompts may still be synced later if autosync is enabled, and a later pull
// may recreate the deleted rows locally.
func (s *SQLiteStore) DeleteSession(id string) error {
	return s.withTx(func(tx *sql.Tx) error {
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

		return nil
	})
}

// ─── Delete Prompt ───────────────────────────────────────────────────────────

// DeletePrompt hard-deletes a single prompt by ID.
// It returns ErrPromptNotFound if no prompt with that ID exists.
//
// Note: this delete only removes local rows. It does not enqueue a delete
// sync mutation, but any previously enqueued mutations for the prompt
// may still be synced later if autosync is enabled, and a later pull
// may recreate the deleted row locally.
func (s *SQLiteStore) DeletePrompt(id int64) error {
	return s.withTx(func(tx *sql.Tx) error {
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
		return nil
	})
}

// ─── Get Single Observation ──────────────────────────────────────────────────

func (s *SQLiteStore) GetObservation(id int64) (*Observation, error) {
	row := s.db.QueryRow(
		`SELECT id, ifnull(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE id = ? AND deleted_at IS NULL`, id,
	)
	var o Observation
	if err := row.Scan(
		&o.ID, &o.SyncID, &o.SessionID, &o.Type, &o.Title, &o.Content,
		&o.ToolName, &o.Project, &o.Scope, &o.TopicKey, &o.RevisionCount, &o.DuplicateCount, &o.LastSeenAt,
		&o.CreatedAt, &o.UpdatedAt, &o.DeletedAt,
	); err != nil {
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
			return nil
		}
		if err != nil {
			return err
		}

		deletedAt := Now()
		if hardDelete {
			if _, err := s.execHook(tx, `DELETE FROM observations WHERE id = ?`, id); err != nil {
				return err
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
			SELECT id, ifnull(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
			       scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
			FROM observations
			WHERE topic_key = ? AND deleted_at IS NULL
		`
		tkArgs := []any{query}

		if opts.Type != "" {
			tkSQL += " AND type = ?"
			tkArgs = append(tkArgs, opts.Type)
		}
		if opts.Project != "" {
			tkSQL += " AND project = ?"
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
					&sr.LastSeenAt, &sr.CreatedAt, &sr.UpdatedAt, &sr.DeletedAt,
				); err != nil {
					break
				}
				sr.Rank = -1000
				directResults = append(directResults, sr)
			}
		}
	}

	// Sanitize query for FTS5 — wrap each term in quotes to avoid syntax errors
	ftsQuery := sanitizeFTS(query)

	sqlQ := `
		SELECT o.id, ifnull(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
		       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.created_at, o.updated_at, o.deleted_at,
		       fts.rank
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
		sqlQ += " AND o.project = ?"
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
			&sr.LastSeenAt, &sr.CreatedAt, &sr.UpdatedAt, &sr.DeletedAt,
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

// ─── Context Formatting ─────────────────────────────────────────────────────

func (s *SQLiteStore) FormatContext(project, scope string) (string, error) {
	sessions, err := s.RecentSessions(project, 5)
	if err != nil {
		return "", err
	}

	observations, err := s.RecentObservations(project, scope, s.cfg.MaxContextResults)
	if err != nil {
		return "", err
	}

	prompts, err := s.RecentPrompts(project, 10)
	if err != nil {
		return "", err
	}

	if len(sessions) == 0 && len(observations) == 0 && len(prompts) == 0 {
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
				sess.Project, sess.StartedAt, summary, sess.ObservationCount)
		}
		b.WriteString("\n")
	}

	if len(prompts) > 0 {
		b.WriteString("### Recent User Prompts\n")
		for _, p := range prompts {
			fmt.Fprintf(&b, "- %s: %s\n", p.CreatedAt, truncate(p.Content, 200))
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
	data := &ExportData{
		Version:    "0.1.0",
		ExportedAt: Now(),
	}

	// Sessions
	rows, err := s.queryItHook(s.db,
		"SELECT id, project, directory, started_at, ended_at, summary FROM sessions ORDER BY started_at",
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
	obsRows, err := s.queryItHook(s.db,
		`SELECT id, ifnull(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations ORDER BY id`,
	)
	if err != nil {
		return nil, fmt.Errorf("export observations: %w", err)
	}
	defer obsRows.Close()
	for obsRows.Next() {
		var o Observation
		if err := obsRows.Scan(
			&o.ID, &o.SyncID, &o.SessionID, &o.Type, &o.Title, &o.Content,
			&o.ToolName, &o.Project, &o.Scope, &o.TopicKey, &o.RevisionCount, &o.DuplicateCount, &o.LastSeenAt,
			&o.CreatedAt, &o.UpdatedAt, &o.DeletedAt,
		); err != nil {
			return nil, err
		}
		data.Observations = append(data.Observations, o)
	}
	if err := obsRows.Err(); err != nil {
		return nil, err
	}

	// Prompts
	promptRows, err := s.queryItHook(s.db,
		"SELECT id, ifnull(sync_id, '') as sync_id, session_id, content, ifnull(project, '') as project, created_at FROM user_prompts ORDER BY id",
	)
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

	// Import observations (use new IDs — AUTOINCREMENT)
	for _, obs := range data.Observations {
		_, err := s.execHook(tx,
			`INSERT INTO observations (sync_id, session_id, type, title, content, tool_name, project, scope, topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			normalizeExistingSyncID(obs.SyncID, "obs"),
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
			obs.CreatedAt,
			obs.UpdatedAt,
			obs.DeletedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("import observation %d: %w", obs.ID, err)
		}
		result.ObservationsImported++
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

// GetSyncedChunks returns a set of chunk IDs that have been imported/exported.
func (s *SQLiteStore) GetSyncedChunks() (map[string]bool, error) {
	rows, err := s.queryItHook(s.db, "SELECT chunk_id FROM sync_chunks")
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

// RecordSyncedChunk marks a chunk as imported/exported so it won't be processed again.
func (s *SQLiteStore) RecordSyncedChunk(chunkID string) error {
	_, err := s.execHook(s.db,
		"INSERT OR IGNORE INTO sync_chunks (chunk_id) VALUES (?)",
		chunkID,
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
		_, err = s.execHook(tx,
			`UPDATE sync_state
			 SET last_acked_seq = ?, lifecycle = ?, updated_at = datetime('now')
			 WHERE target_key = ?`,
			acked, lifecycle, targetKey,
		)
		return err
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
		_, err = s.execHook(tx,
			`UPDATE sync_state SET last_acked_seq = ?, lifecycle = ?, updated_at = datetime('now') WHERE target_key = ?`,
			maxSeq, lifecycle, targetKey,
		)
		return err
	})
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
			 SET lifecycle = ?, consecutive_failures = ?, backoff_until = ?, last_error = ?, updated_at = datetime('now')
			 WHERE target_key = ?`,
			SyncLifecycleDegraded, state.ConsecutiveFailures+1, backoff, message, targetKey,
		)
		return err
	})
}

func (s *SQLiteStore) MarkSyncHealthy(targetKey string) error {
	targetKey = normalizeSyncTargetKey(targetKey)
	_, err := s.execHook(s.db,
		`UPDATE sync_state
		 SET lifecycle = ?, consecutive_failures = 0, backoff_until = NULL, last_error = NULL, updated_at = datetime('now')
		 WHERE target_key = ?`,
		SyncLifecycleHealthy, targetKey,
	)
	return err
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
			// Phase E: per-entity skip+log policy.
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
		return s.applyPromptUpsertTx(tx, payload)
	default:
		return fmt.Errorf("unknown sync entity %q", mutation.Entity)
	}
}

func (s *SQLiteStore) GetObservationBySyncID(syncID string) (*Observation, error) {
	row := s.db.QueryRow(
		`SELECT id, ifnull(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE sync_id = ? AND deleted_at IS NULL ORDER BY id DESC LIMIT 1`,
		syncID,
	)
	var o Observation
	if err := row.Scan(&o.ID, &o.SyncID, &o.SessionID, &o.Type, &o.Title, &o.Content, &o.ToolName, &o.Project, &o.Scope, &o.TopicKey, &o.RevisionCount, &o.DuplicateCount, &o.LastSeenAt, &o.CreatedAt, &o.UpdatedAt, &o.DeletedAt); err != nil {
		return nil, err
	}
	return &o, nil
}

// ─── Project Enrollment for Cloud Sync ───────────────────────────────────────

// EnrollProject registers a project for cloud sync. Idempotent — re-enrolling
// an already-enrolled project is a no-op.
func (s *SQLiteStore) EnrollProject(project string) error {
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
func (s *SQLiteStore) ProjectExists(name string) (bool, error) {
	const query = `
SELECT 1 FROM (
  SELECT project FROM observations WHERE project = ? AND deleted_at IS NULL
  UNION ALL
  SELECT project FROM sessions WHERE project = ?
  UNION ALL
  SELECT project FROM user_prompts WHERE project = ?
  UNION ALL
  SELECT project FROM sync_enrolled_projects WHERE project = ?
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
		for _, src := range sources {
			src, _ = NormalizeProject(src)
			if src == "" || src == canonical {
				continue
			}

			res, err := s.execHook(tx, `UPDATE observations SET project = ? WHERE project = ?`, canonical, src)
			if err != nil {
				return fmt.Errorf("merge observations %q → %q: %w", src, canonical, err)
			}
			n, _ := res.RowsAffected()
			result.ObservationsUpdated += n

			res, err = s.execHook(tx, `UPDATE sessions SET project = ? WHERE project = ?`, canonical, src)
			if err != nil {
				return fmt.Errorf("merge sessions %q → %q: %w", src, canonical, err)
			}
			n, _ = res.RowsAffected()
			result.SessionsUpdated += n

			res, err = s.execHook(tx, `UPDATE user_prompts SET project = ? WHERE project = ?`, canonical, src)
			if err != nil {
				return fmt.Errorf("merge prompts %q → %q: %w", src, canonical, err)
			}
			n, _ = res.RowsAffected()
			result.PromptsUpdated += n

			result.SourcesMerged = append(result.SourcesMerged, src)
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

// ─── Project Pruning ─────────────────────────────────────────────────────────

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

// ─── Helpers ─────────────────────────────────────────────────────────────────

func (s *SQLiteStore) withTx(fn func(tx *sql.Tx) error) error {
	tx, err := s.beginTxHook()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return s.commitHook(tx)
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
		       consecutive_failures, backoff_until, lease_owner, lease_until, last_error, updated_at
		FROM sync_state WHERE target_key = ?`, targetKey)
	var state SyncState
	if err := row.Scan(&state.TargetKey, &state.Lifecycle, &state.LastEnqueuedSeq, &state.LastAckedSeq, &state.LastPulledSeq, &state.ConsecutiveFailures, &state.BackoffUntil, &state.LeaseOwner, &state.LeaseUntil, &state.LastError, &state.UpdatedAt); err != nil {
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
		       consecutive_failures, backoff_until, lease_owner, lease_until, last_error, updated_at
		FROM sync_state WHERE target_key = ?`, targetKey)
	var state SyncState
	if err := row.Scan(&state.TargetKey, &state.Lifecycle, &state.LastEnqueuedSeq, &state.LastAckedSeq, &state.LastPulledSeq, &state.ConsecutiveFailures, &state.BackoffUntil, &state.LeaseOwner, &state.LeaseUntil, &state.LastError, &state.UpdatedAt); err != nil {
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
	return s.backfillPromptSyncMutationsTx(tx, project)
}

func (s *SQLiteStore) repairEnrolledProjectSyncMutations() error {
	return s.withTx(func(tx *sql.Tx) error {
		rows, err := s.queryItHook(tx,
			`SELECT project FROM sync_enrolled_projects ORDER BY project ASC`,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		var projects []string
		for rows.Next() {
			var project string
			if err := rows.Scan(&project); err != nil {
				return err
			}
			projects = append(projects, project)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, project := range projects {
			if err := s.backfillProjectSyncMutationsTx(tx, project); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLiteStore) backfillSessionSyncMutationsTx(tx *sql.Tx, project string) error {
	rows, err := s.queryItHook(tx, `
		SELECT id, project, directory, ended_at, summary
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
	defer rows.Close()

	for rows.Next() {
		var payload syncSessionPayload
		if err := rows.Scan(&payload.ID, &payload.Project, &payload.Directory, &payload.EndedAt, &payload.Summary); err != nil {
			return err
		}
		if err := s.enqueueSyncMutationTx(tx, SyncEntitySession, payload.ID, SyncOpUpsert, payload); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *SQLiteStore) backfillObservationSyncMutationsTx(tx *sql.Tx, project string) error {
	rows, err := s.queryItHook(tx, `
		SELECT sync_id, session_id, type, title, content, tool_name, project, scope, topic_key
		FROM observations
		WHERE ifnull(project, '') = ?
		  AND deleted_at IS NULL
		  AND NOT EXISTS (
			SELECT 1
			FROM sync_mutations sm
			WHERE sm.target_key = ?
			  AND sm.entity = ?
			  AND sm.entity_key = observations.sync_id
			  AND sm.source = ?
		  )
		ORDER BY id ASC`,
		project, DefaultSyncTargetKey, SyncEntityObservation, SyncSourceLocal,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var payload syncObservationPayload
		if err := rows.Scan(&payload.SyncID, &payload.SessionID, &payload.Type, &payload.Title, &payload.Content, &payload.ToolName, &payload.Project, &payload.Scope, &payload.TopicKey); err != nil {
			return err
		}
		if err := s.enqueueSyncMutationTx(tx, SyncEntityObservation, payload.SyncID, SyncOpUpsert, payload); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *SQLiteStore) backfillPromptSyncMutationsTx(tx *sql.Tx, project string) error {
	rows, err := s.queryItHook(tx, `
		SELECT sync_id, session_id, content, project
		FROM user_prompts
		WHERE ifnull(project, '') = ?
		  AND NOT EXISTS (
			SELECT 1
			FROM sync_mutations sm
			WHERE sm.target_key = ?
			  AND sm.entity = ?
			  AND sm.entity_key = user_prompts.sync_id
			  AND sm.source = ?
		  )
		ORDER BY id ASC`,
		project, DefaultSyncTargetKey, SyncEntityPrompt, SyncSourceLocal,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var payload syncPromptPayload
		if err := rows.Scan(&payload.SyncID, &payload.SessionID, &payload.Content, &payload.Project); err != nil {
			return err
		}
		if err := s.enqueueSyncMutationTx(tx, SyncEntityPrompt, payload.SyncID, SyncOpUpsert, payload); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *SQLiteStore) enqueueSyncMutationTx(tx *sql.Tx, entity, entityKey, op string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	project := extractProjectFromPayload(payload)
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
	return err
}

func (s *SQLiteStore) getObservationTx(tx *sql.Tx, id int64) (*Observation, error) {
	row := tx.QueryRow(
		`SELECT id, ifnull(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE id = ? AND deleted_at IS NULL`, id,
	)
	var o Observation
	if err := row.Scan(&o.ID, &o.SyncID, &o.SessionID, &o.Type, &o.Title, &o.Content, &o.ToolName, &o.Project, &o.Scope, &o.TopicKey, &o.RevisionCount, &o.DuplicateCount, &o.LastSeenAt, &o.CreatedAt, &o.UpdatedAt, &o.DeletedAt); err != nil {
		return nil, err
	}
	return &o, nil
}

func (s *SQLiteStore) getObservationBySyncIDTx(tx *sql.Tx, syncID string, includeDeleted bool) (*Observation, error) {
	query := `SELECT id, ifnull(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE sync_id = ?`
	if !includeDeleted {
		query += ` AND deleted_at IS NULL`
	}
	query += ` ORDER BY id DESC LIMIT 1`
	row := tx.QueryRow(query, syncID)
	var o Observation
	if err := row.Scan(&o.ID, &o.SyncID, &o.SessionID, &o.Type, &o.Title, &o.Content, &o.ToolName, &o.Project, &o.Scope, &o.TopicKey, &o.RevisionCount, &o.DuplicateCount, &o.LastSeenAt, &o.CreatedAt, &o.UpdatedAt, &o.DeletedAt); err != nil {
		return nil, err
	}
	return &o, nil
}

func (s *SQLiteStore) applySessionPayloadTx(tx *sql.Tx, payload syncSessionPayload) error {
	_, err := s.execHook(tx,
		`INSERT INTO sessions (id, project, directory, ended_at, summary)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   project = excluded.project,
		   directory = excluded.directory,
		   ended_at = COALESCE(excluded.ended_at, sessions.ended_at),
		   summary = COALESCE(excluded.summary, sessions.summary)`,
		payload.ID, payload.Project, payload.Directory, payload.EndedAt, payload.Summary,
	)
	return err
}

func (s *SQLiteStore) applyObservationUpsertTx(tx *sql.Tx, payload syncObservationPayload) error {
	existing, err := s.getObservationBySyncIDTx(tx, payload.SyncID, true)
	if err == sql.ErrNoRows {
		_, err = s.execHook(tx,
			`INSERT INTO observations (sync_id, session_id, type, title, content, tool_name, project, scope, topic_key, normalized_hash, revision_count, duplicate_count, updated_at, deleted_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, datetime('now'), NULL)`,
			payload.SyncID, payload.SessionID, payload.Type, payload.Title, payload.Content, payload.ToolName, payload.Project, normalizeScope(payload.Scope), payload.TopicKey, hashNormalized(payload.Content),
		)
		return err
	}
	if err != nil {
		return err
	}
	_, err = s.execHook(tx,
		`UPDATE observations
		 SET session_id = ?, type = ?, title = ?, content = ?, tool_name = ?, project = ?, scope = ?, topic_key = ?, normalized_hash = ?, revision_count = revision_count + 1, updated_at = datetime('now'), deleted_at = NULL
		 WHERE id = ?`,
		payload.SessionID, payload.Type, payload.Title, payload.Content, payload.ToolName, payload.Project, normalizeScope(payload.Scope), payload.TopicKey, hashNormalized(payload.Content), existing.ID,
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
	var existingID int64
	err := tx.QueryRow(`SELECT id FROM user_prompts WHERE sync_id = ? ORDER BY id DESC LIMIT 1`, payload.SyncID).Scan(&existingID)
	if err == sql.ErrNoRows {
		_, err = s.execHook(tx,
			`INSERT INTO user_prompts (sync_id, session_id, content, project) VALUES (?, ?, ?, ?)`,
			payload.SyncID, payload.SessionID, payload.Content, payload.Project,
		)
		return err
	}
	if err != nil {
		return err
	}
	_, err = s.execHook(tx,
		`UPDATE user_prompts SET session_id = ?, content = ?, project = ? WHERE id = ?`,
		payload.SessionID, payload.Content, payload.Project, existingID,
	)
	return err
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
		if err := rows.Scan(
			&o.ID, &o.SyncID, &o.SessionID, &o.Type, &o.Title, &o.Content,
			&o.ToolName, &o.Project, &o.Scope, &o.TopicKey, &o.RevisionCount, &o.DuplicateCount, &o.LastSeenAt,
			&o.CreatedAt, &o.UpdatedAt, &o.DeletedAt,
		); err != nil {
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
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == columnName {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	_, err = s.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, columnName, definition))
	return err
}

func (s *SQLiteStore) migrateLegacyObservationsTable() error {
	rows, err := s.queryItHook(s.db, "PRAGMA table_info(observations)")
	if err != nil {
		return err
	}
	defer rows.Close()

	var hasID bool
	var idIsPrimaryKey bool
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == "id" {
			hasID = true
			idIsPrimaryKey = pk == 1
			break
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if !hasID || idIsPrimaryKey {
		return nil
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
			created_at TEXT    NOT NULL DEFAULT (datetime('now')),
			updated_at TEXT    NOT NULL DEFAULT (datetime('now')),
			deleted_at TEXT,
			FOREIGN KEY (session_id) REFERENCES sessions(id)
		);
	`); err != nil {
		return fmt.Errorf("migrate legacy observations: create table: %w", err)
	}

	if _, err := s.execHook(tx, `
		INSERT INTO observations_migrated (
			id, sync_id, session_id, type, title, content, tool_name, project,
			scope, topic_key, normalized_hash, revision_count, duplicate_count,
			last_seen_at, created_at, updated_at, deleted_at
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
			COALESCE(NULLIF(created_at, ''), datetime('now')),
			COALESCE(NULLIF(updated_at, ''), NULLIF(created_at, ''), datetime('now')),
			deleted_at
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
// SQLite stub — does not have per-row created_by in all builds, returns empty.
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


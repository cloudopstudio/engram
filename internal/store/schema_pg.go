package store

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ftsBaseLanguage returns the base PostgreSQL text search language.
// Defaults to 'english', overridable via ENGRAM_FTS_LANGUAGE env var.
// Used during migration to create the 'engram' text search configuration.
func ftsBaseLanguage() string {
	if lang := os.Getenv("ENGRAM_FTS_LANGUAGE"); lang != "" {
		return lang
	}
	return "english"
}

// ftsLanguage returns the PostgreSQL text search configuration to use
// in queries. After migration v8, this is the custom 'engram' config
// which includes unaccent + language stemmer for diacritic-insensitive
// search (código = codigo).
func ftsLanguage() string {
	return "engram"
}

// migratePG runs idempotent schema migrations against the PostgreSQL database.
// It creates all tables, indexes, triggers, and extensions needed.
func migratePG(pool *pgxpool.Pool) error {
	ctx := context.Background()

	// ── Schema migrations table ──
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INTEGER PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	migrations := []struct {
		version     int
		description string
		sql         string
	}{
		{
			version:     1,
			description: "core tables: sessions, observations, user_prompts",
			sql: `
				CREATE TABLE IF NOT EXISTS sessions (
					id         TEXT PRIMARY KEY,
					project    TEXT NOT NULL,
					directory  TEXT NOT NULL,
					started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					ended_at   TIMESTAMPTZ,
					summary    TEXT,
					created_by TEXT NOT NULL DEFAULT ''
				);

				CREATE INDEX IF NOT EXISTS idx_sessions_project  ON sessions(project);
				CREATE INDEX IF NOT EXISTS idx_sessions_started   ON sessions(started_at DESC);

				CREATE TABLE IF NOT EXISTS observations (
					id              BIGSERIAL PRIMARY KEY,
					sync_id         TEXT UNIQUE,
					session_id      TEXT NOT NULL REFERENCES sessions(id),
					type            TEXT NOT NULL,
					title           TEXT NOT NULL,
					content         TEXT NOT NULL,
					tool_name       TEXT,
					project         TEXT,
					scope           TEXT NOT NULL DEFAULT 'project',
					topic_key       TEXT,
					normalized_hash TEXT,
					revision_count  INTEGER NOT NULL DEFAULT 1,
					duplicate_count INTEGER NOT NULL DEFAULT 1,
					last_seen_at    TIMESTAMPTZ,
					created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					deleted_at      TIMESTAMPTZ,
					created_by      TEXT NOT NULL DEFAULT '',
					search_vector   tsvector
				);

				CREATE INDEX IF NOT EXISTS idx_obs_session  ON observations(session_id);
				CREATE INDEX IF NOT EXISTS idx_obs_type     ON observations(type);
				CREATE INDEX IF NOT EXISTS idx_obs_project  ON observations(project);
				CREATE INDEX IF NOT EXISTS idx_obs_created  ON observations(created_at DESC);
				CREATE INDEX IF NOT EXISTS idx_obs_scope    ON observations(scope);
				CREATE INDEX IF NOT EXISTS idx_obs_sync_id  ON observations(sync_id);
				CREATE INDEX IF NOT EXISTS idx_obs_topic    ON observations(topic_key, project, scope, updated_at DESC);
				CREATE INDEX IF NOT EXISTS idx_obs_deleted  ON observations(deleted_at);
				CREATE INDEX IF NOT EXISTS idx_obs_dedupe   ON observations(normalized_hash, project, scope, type, title, created_at DESC);
				CREATE INDEX IF NOT EXISTS idx_obs_created_by ON observations(created_by);
				CREATE INDEX IF NOT EXISTS idx_obs_fts      ON observations USING GIN(search_vector);

				CREATE TABLE IF NOT EXISTS user_prompts (
					id          BIGSERIAL PRIMARY KEY,
					sync_id     TEXT UNIQUE,
					session_id  TEXT NOT NULL REFERENCES sessions(id),
					content     TEXT NOT NULL,
					project     TEXT,
					created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					created_by  TEXT NOT NULL DEFAULT '',
					search_vector tsvector
				);

				CREATE INDEX IF NOT EXISTS idx_prompts_session  ON user_prompts(session_id);
				CREATE INDEX IF NOT EXISTS idx_prompts_project  ON user_prompts(project);
				CREATE INDEX IF NOT EXISTS idx_prompts_created  ON user_prompts(created_at DESC);
				CREATE INDEX IF NOT EXISTS idx_prompts_sync_id  ON user_prompts(sync_id);
				CREATE INDEX IF NOT EXISTS idx_prompts_fts      ON user_prompts USING GIN(search_vector);
			`,
		},
		{
			version:     2,
			description: "tsvector trigger functions for observations and prompts",
			sql: fmt.Sprintf(`
				CREATE OR REPLACE FUNCTION observations_search_vector_update() RETURNS trigger AS $$
				BEGIN
					NEW.search_vector :=
						setweight(to_tsvector('%s', COALESCE(NEW.title, '')), 'A') ||
						setweight(to_tsvector('%s', COALESCE(NEW.topic_key, '')), 'A') ||
						setweight(to_tsvector('%s', COALESCE(NEW.content, '')), 'B') ||
						setweight(to_tsvector('%s', COALESCE(NEW.type, '')), 'C') ||
						setweight(to_tsvector('%s', COALESCE(NEW.project, '')), 'C') ||
						setweight(to_tsvector('%s', COALESCE(NEW.tool_name, '')), 'D');
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				DROP TRIGGER IF EXISTS trg_obs_search_vector ON observations;
				CREATE TRIGGER trg_obs_search_vector
					BEFORE INSERT OR UPDATE ON observations
					FOR EACH ROW EXECUTE FUNCTION observations_search_vector_update();

				CREATE OR REPLACE FUNCTION prompts_search_vector_update() RETURNS trigger AS $$
				BEGIN
					NEW.search_vector :=
						setweight(to_tsvector('%s', COALESCE(NEW.content, '')), 'A') ||
						setweight(to_tsvector('%s', COALESCE(NEW.project, '')), 'B');
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				DROP TRIGGER IF EXISTS trg_prompts_search_vector ON user_prompts;
				CREATE TRIGGER trg_prompts_search_vector
					BEFORE INSERT OR UPDATE ON user_prompts
					FOR EACH ROW EXECUTE FUNCTION prompts_search_vector_update();
			`, ftsBaseLanguage(), ftsBaseLanguage(), ftsBaseLanguage(), ftsBaseLanguage(), ftsBaseLanguage(), ftsBaseLanguage(),
				ftsBaseLanguage(), ftsBaseLanguage()),
		},
		{
			version:     3,
			description: "sync tables: sync_chunks, sync_state, sync_mutations, sync_enrolled_projects",
			sql: `
				CREATE TABLE IF NOT EXISTS sync_chunks (
					chunk_id    TEXT PRIMARY KEY,
					imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
				);

				CREATE TABLE IF NOT EXISTS sync_state (
					target_key           TEXT PRIMARY KEY,
					lifecycle            TEXT NOT NULL DEFAULT 'idle',
					last_enqueued_seq    BIGINT NOT NULL DEFAULT 0,
					last_acked_seq       BIGINT NOT NULL DEFAULT 0,
					last_pulled_seq      BIGINT NOT NULL DEFAULT 0,
					consecutive_failures INTEGER NOT NULL DEFAULT 0,
					backoff_until        TIMESTAMPTZ,
					lease_owner          TEXT,
					lease_until          TIMESTAMPTZ,
					last_error           TEXT,
					updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
				);

				CREATE TABLE IF NOT EXISTS sync_mutations (
					seq         BIGSERIAL PRIMARY KEY,
					target_key  TEXT NOT NULL REFERENCES sync_state(target_key),
					entity      TEXT NOT NULL,
					entity_key  TEXT NOT NULL,
					op          TEXT NOT NULL,
					payload     JSONB NOT NULL,
					source      TEXT NOT NULL DEFAULT 'local',
					project     TEXT NOT NULL DEFAULT '',
					occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					acked_at    TIMESTAMPTZ
				);

				CREATE INDEX IF NOT EXISTS idx_sync_mutations_target_seq ON sync_mutations(target_key, seq);
				CREATE INDEX IF NOT EXISTS idx_sync_mutations_pending    ON sync_mutations(target_key, acked_at, seq);
				CREATE INDEX IF NOT EXISTS idx_sync_mutations_project    ON sync_mutations(project);

				CREATE TABLE IF NOT EXISTS sync_enrolled_projects (
					project     TEXT PRIMARY KEY,
					enrolled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
				);

				-- Ensure default sync state exists.
				INSERT INTO sync_state (target_key, lifecycle, updated_at)
				VALUES ('cloud', 'idle', NOW())
				ON CONFLICT(target_key) DO NOTHING;
			`,
		},
		{
			version:     4,
			description: "pgvector extension and embedding column (optional)",
			sql:         "", // Handled separately below — may fail gracefully.
		},
		{
			version:     5,
			description: "row-level security for personal scope isolation",
			sql: `
				-- Backfill created_by with current_user where it was left empty.
				UPDATE observations SET created_by = current_user WHERE created_by = '';
				UPDATE user_prompts SET created_by = current_user WHERE created_by = '';

				-- Change the default so PG auto-fills created_by on raw INSERTs.
				ALTER TABLE observations ALTER COLUMN created_by SET DEFAULT current_user;
				ALTER TABLE user_prompts ALTER COLUMN created_by SET DEFAULT current_user;

				-- Enable RLS on both tables.
				ALTER TABLE observations ENABLE ROW LEVEL SECURITY;
				ALTER TABLE observations FORCE ROW LEVEL SECURITY;

				ALTER TABLE user_prompts ENABLE ROW LEVEL SECURITY;
				ALTER TABLE user_prompts FORCE ROW LEVEL SECURITY;

				-- ── Observations policies ──

				-- SELECT: project scope visible to all; personal only to creator.
				DROP POLICY IF EXISTS obs_visibility ON observations;
				CREATE POLICY obs_visibility ON observations
					FOR SELECT
					USING (scope = 'project' OR scope IS NULL OR (scope = 'personal' AND created_by = current_user));

				-- INSERT: anyone can insert (created_by auto-set by DEFAULT current_user).
				DROP POLICY IF EXISTS obs_insert ON observations;
				CREATE POLICY obs_insert ON observations
					FOR INSERT
					WITH CHECK (true);

				-- UPDATE: project scope by anyone; personal only by creator.
				DROP POLICY IF EXISTS obs_modify ON observations;
				CREATE POLICY obs_modify ON observations
					FOR UPDATE
					USING (scope = 'project' OR (scope = 'personal' AND created_by = current_user));

				-- DELETE: project scope by anyone; personal only by creator.
				DROP POLICY IF EXISTS obs_delete ON observations;
				CREATE POLICY obs_delete ON observations
					FOR DELETE
					USING (scope = 'project' OR (scope = 'personal' AND created_by = current_user));

				-- ── User prompts policies ──
				-- Prompts do not have a scope column; they are always personal.
				-- Only the creator can see/modify their own prompts.

				DROP POLICY IF EXISTS prompts_visibility ON user_prompts;
				CREATE POLICY prompts_visibility ON user_prompts
					FOR SELECT
					USING (created_by = current_user);

				DROP POLICY IF EXISTS prompts_insert ON user_prompts;
				CREATE POLICY prompts_insert ON user_prompts
					FOR INSERT
					WITH CHECK (true);

				DROP POLICY IF EXISTS prompts_modify ON user_prompts;
				CREATE POLICY prompts_modify ON user_prompts
					FOR UPDATE
					USING (created_by = current_user);

				DROP POLICY IF EXISTS prompts_delete ON user_prompts;
				CREATE POLICY prompts_delete ON user_prompts
					FOR DELETE
					USING (created_by = current_user);
			`,
		},
		{
			version:     6,
			description: "RLS policies use engram.identity GUC instead of current_user",
			sql: `
				-- The engram.identity GUC is set per-connection via AfterConnect
				-- with the Entra ID UPN (e.g. user@company.com). This allows RLS
				-- to distinguish users even when they share the same PG role.
				--
				-- current_setting('engram.identity', true) returns NULL if the GUC
				-- is not set (e.g. password auth without Entra), which makes the
				-- COALESCE fall back to current_user for backward compatibility.

				-- Helper: resolve the effective identity for RLS.
				CREATE OR REPLACE FUNCTION engram_current_identity() RETURNS TEXT AS $$
					SELECT COALESCE(
						NULLIF(current_setting('engram.identity', true), ''),
						current_user
					);
				$$ LANGUAGE sql STABLE;

				-- ── Observations policies ──

				DROP POLICY IF EXISTS obs_visibility ON observations;
				CREATE POLICY obs_visibility ON observations
					FOR SELECT
					USING (
						scope = 'project'
						OR scope IS NULL
						OR (scope = 'personal' AND created_by = engram_current_identity())
					);

				DROP POLICY IF EXISTS obs_insert ON observations;
				CREATE POLICY obs_insert ON observations
					FOR INSERT
					WITH CHECK (true);

				DROP POLICY IF EXISTS obs_modify ON observations;
				CREATE POLICY obs_modify ON observations
					FOR UPDATE
					USING (
						scope = 'project'
						OR (scope = 'personal' AND created_by = engram_current_identity())
					);

				DROP POLICY IF EXISTS obs_delete ON observations;
				CREATE POLICY obs_delete ON observations
					FOR DELETE
					USING (
						scope = 'project'
						OR (scope = 'personal' AND created_by = engram_current_identity())
					);

				-- ── User prompts policies ──

				DROP POLICY IF EXISTS prompts_visibility ON user_prompts;
				CREATE POLICY prompts_visibility ON user_prompts
					FOR SELECT
					USING (created_by = engram_current_identity());

				DROP POLICY IF EXISTS prompts_insert ON user_prompts;
				CREATE POLICY prompts_insert ON user_prompts
					FOR INSERT
					WITH CHECK (true);

				DROP POLICY IF EXISTS prompts_modify ON user_prompts;
				CREATE POLICY prompts_modify ON user_prompts
					FOR UPDATE
					USING (created_by = engram_current_identity());

				DROP POLICY IF EXISTS prompts_delete ON user_prompts;
				CREATE POLICY prompts_delete ON user_prompts
					FOR DELETE
					USING (created_by = engram_current_identity());

				-- Backfill: update empty created_by with current_user as best effort.
				UPDATE observations SET created_by = current_user WHERE created_by = '';
				UPDATE user_prompts SET created_by = current_user WHERE created_by = '';
			`,
		},
		{
			version:     7,
			description: "project_metadata table for project deprecation",
			sql: `
				CREATE TABLE IF NOT EXISTS project_metadata (
					project       TEXT PRIMARY KEY,
					deprecated    BOOLEAN NOT NULL DEFAULT false,
					deprecated_at TIMESTAMPTZ,
					deprecated_by TEXT,
					description   TEXT,
					created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
				);
			`,
		},
		{
			version:     8,
			description: "unaccent-aware FTS: normalize diacritics (código = codigo)",
			sql: fmt.Sprintf(`
				-- Enable unaccent extension for diacritics normalization.
				CREATE EXTENSION IF NOT EXISTS unaccent;

				-- Create a custom text search config that strips diacritics
				-- before stemming. This makes "código" match "codigo", "ñoño"
				-- match "nono", etc. — critical for Spanish and Portuguese.
				DROP TEXT SEARCH CONFIGURATION IF EXISTS engram;
				CREATE TEXT SEARCH CONFIGURATION engram (COPY = %s);
				ALTER TEXT SEARCH CONFIGURATION engram
					ALTER MAPPING FOR asciiword, asciihword, hword_asciipart,
					                  word, hword, hword_part
					WITH unaccent, %s_stem;

				-- Recreate trigger functions using the new 'engram' config
				-- instead of the bare language name.
				CREATE OR REPLACE FUNCTION observations_search_vector_update() RETURNS trigger AS $$
				BEGIN
					NEW.search_vector :=
						setweight(to_tsvector('engram', COALESCE(NEW.title, '')), 'A') ||
						setweight(to_tsvector('engram', COALESCE(NEW.topic_key, '')), 'A') ||
						setweight(to_tsvector('engram', COALESCE(NEW.content, '')), 'B') ||
						setweight(to_tsvector('engram', COALESCE(NEW.type, '')), 'C') ||
						setweight(to_tsvector('engram', COALESCE(NEW.project, '')), 'C') ||
						setweight(to_tsvector('engram', COALESCE(NEW.tool_name, '')), 'D');
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				CREATE OR REPLACE FUNCTION prompts_search_vector_update() RETURNS trigger AS $$
				BEGIN
					NEW.search_vector :=
						setweight(to_tsvector('engram', COALESCE(NEW.content, '')), 'A') ||
						setweight(to_tsvector('engram', COALESCE(NEW.project, '')), 'B');
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				-- Reindex ALL existing rows. Concatenating empty string forces
				-- PostgreSQL to see the column as changed, firing the trigger.
				UPDATE observations SET title = title || '' WHERE deleted_at IS NULL;
				UPDATE user_prompts SET content = content || '';
			`, ftsBaseLanguage(), ftsBaseLanguage()),
		},
		{
			version:     9,
			description: "fix v8: recreate triggers with engram config and reindex existing rows",
			sql: `
				-- v8 only created the text search config but did not update the
				-- trigger functions or reindex. This migration fixes that.

				CREATE OR REPLACE FUNCTION observations_search_vector_update() RETURNS trigger AS $$
				BEGIN
					NEW.search_vector :=
						setweight(to_tsvector('engram', COALESCE(NEW.title, '')), 'A') ||
						setweight(to_tsvector('engram', COALESCE(NEW.topic_key, '')), 'A') ||
						setweight(to_tsvector('engram', COALESCE(NEW.content, '')), 'B') ||
						setweight(to_tsvector('engram', COALESCE(NEW.type, '')), 'C') ||
						setweight(to_tsvector('engram', COALESCE(NEW.project, '')), 'C') ||
						setweight(to_tsvector('engram', COALESCE(NEW.tool_name, '')), 'D');
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				CREATE OR REPLACE FUNCTION prompts_search_vector_update() RETURNS trigger AS $$
				BEGIN
					NEW.search_vector :=
						setweight(to_tsvector('engram', COALESCE(NEW.content, '')), 'A') ||
						setweight(to_tsvector('engram', COALESCE(NEW.project, '')), 'B');
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;

				-- Reindex: concatenating '' forces trigger to fire.
				UPDATE observations SET title = title || '' WHERE deleted_at IS NULL;
				UPDATE user_prompts SET content = content || '';
			`,
		},
		{
			version:     10,
			description: "add memory_relations table and sync_apply_deferred for conflict surfacing",
			sql: `
				CREATE TABLE IF NOT EXISTS memory_relations (
					id                        BIGSERIAL PRIMARY KEY,
					sync_id                   TEXT      NOT NULL UNIQUE,
					source_id                 TEXT,
					target_id                 TEXT,
					relation                  TEXT      NOT NULL DEFAULT 'pending',
					reason                    TEXT,
					evidence                  TEXT,
					confidence                DOUBLE PRECISION,
					judgment_status           TEXT      NOT NULL DEFAULT 'pending',
					marked_by_actor           TEXT,
					marked_by_kind            TEXT,
					marked_by_model           TEXT,
					session_id                TEXT,
					superseded_at             TIMESTAMPTZ,
					superseded_by_relation_id BIGINT REFERENCES memory_relations(id) ON DELETE SET NULL,
					created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
				);

				CREATE INDEX IF NOT EXISTS idx_memrel_source         ON memory_relations(source_id, judgment_status);
				CREATE INDEX IF NOT EXISTS idx_memrel_target         ON memory_relations(target_id, judgment_status);
				CREATE INDEX IF NOT EXISTS idx_memrel_supersede      ON memory_relations(superseded_by_relation_id);
				CREATE INDEX IF NOT EXISTS idx_memrel_status_created ON memory_relations(judgment_status, created_at DESC);

				CREATE TABLE IF NOT EXISTS sync_apply_deferred (
					sync_id           TEXT        PRIMARY KEY,
					entity            TEXT        NOT NULL,
					payload           TEXT        NOT NULL,
					apply_status      TEXT        NOT NULL DEFAULT 'deferred',
					retry_count       INTEGER     NOT NULL DEFAULT 0,
					last_error        TEXT,
					last_attempted_at TIMESTAMPTZ,
					first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
				);

				CREATE INDEX IF NOT EXISTS idx_sad_status_seen ON sync_apply_deferred(apply_status, first_seen_at);
			`,
		},
		{
			version:     11,
			description: "decay scheduling columns on observations (review_after, expires_at)",
			sql: `
				-- Phase decay-v1: review_after holds the ISO-8601 timestamp after which
				-- this observation should be reviewed/refreshed.
				-- expires_at is reserved for Phase 2 and remains NULL.
				ALTER TABLE observations ADD COLUMN IF NOT EXISTS review_after TIMESTAMPTZ;
				ALTER TABLE observations ADD COLUMN IF NOT EXISTS expires_at   TIMESTAMPTZ;
			`,
		},
	}

	for _, m := range migrations {
		// Check if already applied.
		var exists bool
		if err := pool.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)",
			m.version,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check migration v%d: %w", m.version, err)
		}
		if exists {
			continue
		}

		// Special handling for pgvector (v4) — graceful failure.
		if m.version == 4 {
			if err := migratePGVector(pool); err != nil {
				log.Printf("[engram] pgvector extension not available (non-fatal): %v", err)
			}
			// Record as applied regardless (we tried).
			if _, err := pool.Exec(ctx,
				"INSERT INTO schema_migrations (version, description) VALUES ($1, $2)",
				m.version, m.description,
			); err != nil {
				return fmt.Errorf("record migration v%d: %w", m.version, err)
			}
			continue
		}

		// Apply the migration.
		if _, err := pool.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("apply migration v%d (%s): %w", m.version, m.description, err)
		}

		// Record it.
		if _, err := pool.Exec(ctx,
			"INSERT INTO schema_migrations (version, description) VALUES ($1, $2)",
			m.version, m.description,
		); err != nil {
			return fmt.Errorf("record migration v%d: %w", m.version, err)
		}
	}

	return nil
}

// migratePGVector attempts to enable the pgvector extension and add the
// embedding column. Non-fatal if the extension isn't available.
func migratePGVector(pool *pgxpool.Pool) error {
	ctx := context.Background()

	if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector"); err != nil {
		return fmt.Errorf("create vector extension: %w", err)
	}

	// Add embedding column if it doesn't exist.
	var colExists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'observations' AND column_name = 'embedding'
		)
	`).Scan(&colExists); err != nil {
		return fmt.Errorf("check embedding column: %w", err)
	}

	if !colExists {
		if _, err := pool.Exec(ctx,
			"ALTER TABLE observations ADD COLUMN embedding vector(1536)",
		); err != nil {
			return fmt.Errorf("add embedding column: %w", err)
		}
	}

	return nil
}

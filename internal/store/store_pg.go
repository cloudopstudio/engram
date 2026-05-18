// Package store implements the persistent memory engine for Engram.
//
// This file provides the PostgreSQL backend, activated via the `pgstore`
// build tag. It implements the same public API as the SQLite store
// (store.go) so consumers (MCP, HTTP, TUI, CLI) need zero changes.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Gentleman-Programming/engram/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Types ───────────────────────────────────────────────────────────────────
//
// Public types (Session, Observation, SearchResult, etc.) and the Config
// struct now live in types.go and config.go (no build tags) so they are
// shared with the SQLite backend.
//
// Sentinel errors (ErrSessionNotFound, ErrSessionHasObservations,
// ErrPromptNotFound) now live in errors.go.
//
// Sync-related constants (DefaultSyncTargetKey, SyncLifecycle*, SyncEntity*,
// SyncOp*, SyncSource*) now live in sync_constants.go.

// ─── Store ───────────────────────────────────────────────────────────────────

// PostgresStore is the PostgreSQL-backed implementation of the Store interface.
type PostgresStore struct {
	pool     *pgxpool.Pool
	cfg      Config
	identity string // Entra ID email, populated if auth method is entra.
}

// NewPostgresStore creates a PG-backed Store by reading ENGRAM_DATABASE_URL.
// It returns the Store interface so callers depend on behavior rather than
// the concrete *PostgresStore type.
func NewPostgresStore(cfg Config) (Store, error) {
	connStr := os.Getenv("ENGRAM_DATABASE_URL")
	if connStr == "" {
		if v, err := config.GetWithProfile(cfg.DataDir, cfg.Profile, "database-url"); err == nil && v != "" {
			connStr = v
		}
	}
	if connStr == "" {
		return nil, fmt.Errorf("engram: database-url not configured. Set ENGRAM_DATABASE_URL or run: engram config set database-url <url>")
	}

	authMethod := resolveAuthMethod(connStr, cfg.DataDir, cfg.Profile)

	var ts TokenSource
	var identity string

	switch authMethod {
	case "aws-iam":
		region, awsProfile := resolveAWSAuth(cfg.DataDir, cfg.Profile)
		awsTP, err := NewAWSTokenProvider(context.Background(), connStr, region, awsProfile)
		if err != nil {
			return nil, fmt.Errorf("engram: %w", err)
		}
		ts = awsTP
		identity = awsTP.Identity()
		log.Printf("[engram] aws-iam auth (region=%s profile=%s identity=%s)", awsTP.region, awsProfile, identity)

	case "entra":
		var tp *TokenProvider
		if cfg.AuthInteractive {
			// Explicit --auth-interactive: use cached token only — never block
			// with device code. The device code flow is reserved for `engram login`.
			// If the token is expired but has a refresh token, use refreshableCredential
			// so it can silently renew without user interaction.
			if err := ValidateCachedToken(cfg.DataDir, cfg.Profile); err != nil {
				return nil, fmt.Errorf("engram: %v", err)
			}
			cached, _ := loadCachedToken(cfg.DataDir)
			tp = newTokenProviderFromCache(cached, cfg.DataDir)
		} else {
			// Credential chain: try DefaultAzureCredential first (az login,
			// managed identity, env vars), then fall back to token-cache.json
			// (from `engram login` / `/engram-login`). This lets both technical
			// users (az login) and non-technical users (/engram-login in OpenCode)
			// work with the same MCP configuration — no flags needed.
			var defaultCredErr error
			tp, defaultCredErr = NewTokenProvider()
			if defaultCredErr == nil {
				// Verify DefaultAzureCredential actually works (az login may be expired).
				if _, err := tp.Token(context.Background()); err != nil {
					log.Printf("[engram] DefaultAzureCredential failed, trying token cache: %v", err)
					tp = nil // fall through to cache path
				}
			} else {
				log.Printf("[engram] DefaultAzureCredential unavailable, trying token cache: %v", defaultCredErr)
			}

			// Fallback: token-cache.json from `engram login` or `/engram-login`.
			if tp == nil {
				if err := ValidateCachedToken(cfg.DataDir, cfg.Profile); err != nil {
					return nil, fmt.Errorf("engram: no valid Azure credentials found.\n\n"+
						"  Option A (recommended): az login --scope %q\n"+
						"  Option B (OpenCode):    /engram-login\n\n"+
						"Details — DefaultAzureCredential: %v", pgTokenScope, defaultCredErr)
				}
				cached, _ := loadCachedToken(cfg.DataDir)
				tp = newTokenProviderFromCache(cached, cfg.DataDir)
				if cached.RefreshToken != "" {
					log.Printf("[engram] using refreshable cached token (access expires %s)", cached.ExpiresOn.Format("15:04:05"))
				} else {
					log.Printf("[engram] using cached token (expires %s)", cached.ExpiresOn.Format("15:04:05"))
				}
			}
		}
		// Acquire an initial token to populate identity.
		if _, err := tp.Token(context.Background()); err != nil {
			log.Printf("[engram] warning: initial token acquisition failed: %v", err)
		} else {
			identity = tp.Identity()
		}
		ts = tp

	case "password":
		// No token source — pgx will use the password from the connection string.

	default:
		return nil, fmt.Errorf("engram: unknown auth-method %q (valid: entra, aws-iam, password)", authMethod)
	}

	pgxCfg, err := configurePGPool(connStr, ts)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), pgxCfg)
	if err != nil {
		return nil, fmt.Errorf("engram: create pg pool: %w", err)
	}

	// Verify connectivity.
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("engram: PostgreSQL unreachable: %w", err)
	}

	// Run schema migrations.
	if err := migratePG(pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("engram: pg migration: %w", err)
	}

	s := &PostgresStore{pool: pool, cfg: cfg, identity: identity}
	if err := s.repairEnrolledProjectSyncMutations(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("engram: repair enrolled sync journal: %w", err)
	}

	return s, nil
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

// Identity returns the Entra ID identity (UPN) associated with the store.
func (s *PostgresStore) Identity() string {
	return s.identity
}

func (s *PostgresStore) MaxObservationLength() int {
	return s.cfg.MaxObservationLength
}

// ─── Transaction helper ──────────────────────────────────────────────────────

func (s *PostgresStore) withTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ─── Timestamp helpers ───────────────────────────────────────────────────────
//
// tsFormat and Now() now live in time_helpers.go (no build tags) so they are
// shared between the SQLite and PostgreSQL backends.

func formatTS(t time.Time) string {
	return t.UTC().Format(tsFormat)
}

func formatNullableTS(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := formatTS(*t)
	return &s
}

// ─── Sessions ────────────────────────────────────────────────────────────────

func (s *PostgresStore) CreateSession(id, project, directory string) error {
	ctx := context.Background()
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if err := s.createSessionTx(ctx, tx, id, project, directory); err != nil {
			return err
		}
		return s.enqueueSyncMutationTx(ctx, tx, SyncEntitySession, id, SyncOpUpsert, syncSessionPayload{
			ID:        id,
			Project:   project,
			Directory: directory,
		})
	})
}

func (s *PostgresStore) createSessionTx(ctx context.Context, tx pgx.Tx, id, project, directory string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO sessions (id, project, directory, created_by) VALUES ($1, $2, $3, $4)
		 ON CONFLICT(id) DO UPDATE SET
		   project   = CASE WHEN sessions.project = '' THEN EXCLUDED.project ELSE sessions.project END,
		   directory = CASE WHEN sessions.directory = '' THEN EXCLUDED.directory ELSE sessions.directory END`,
		id, project, directory, s.identity,
	)
	return err
}

func (s *PostgresStore) EndSession(id string, summary string) error {
	ctx := context.Background()
	return s.withTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE sessions SET ended_at = NOW(), summary = $1 WHERE id = $2`,
			nullableString(summary), id,
		)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}

		var project, directory string
		var endedAt time.Time
		var storedSummary *string
		if err := tx.QueryRow(ctx,
			`SELECT project, directory, ended_at, summary FROM sessions WHERE id = $1`, id,
		).Scan(&project, &directory, &endedAt, &storedSummary); err != nil {
			return err
		}

		endedStr := formatTS(endedAt)
		return s.enqueueSyncMutationTx(ctx, tx, SyncEntitySession, id, SyncOpUpsert, syncSessionPayload{
			ID:        id,
			Project:   project,
			Directory: directory,
			EndedAt:   &endedStr,
			Summary:   storedSummary,
		})
	})
}

func (s *PostgresStore) GetSession(id string) (*Session, error) {
	ctx := context.Background()
	var sess Session
	var startedAt time.Time
	var endedAt *time.Time
	if err := s.pool.QueryRow(ctx,
		`SELECT id, project, directory, started_at, ended_at, summary FROM sessions WHERE id = $1`, id,
	).Scan(&sess.ID, &sess.Project, &sess.Directory, &startedAt, &endedAt, &sess.Summary); err != nil {
		return nil, err
	}
	sess.StartedAt = formatTS(startedAt)
	sess.EndedAt = formatNullableTS(endedAt)
	return &sess, nil
}

func (s *PostgresStore) RecentSessions(project string, limit int) ([]SessionSummary, error) {
	if limit <= 0 {
		limit = 5
	}
	ctx := context.Background()

	query := `
		SELECT s.id, s.project, s.started_at, s.ended_at, s.summary,
		       COUNT(o.id) as observation_count
		FROM sessions s
		LEFT JOIN observations o ON o.session_id = s.id AND o.deleted_at IS NULL
		WHERE TRUE
	`
	args := []any{}
	argN := 1

	if project != "" {
		query += fmt.Sprintf(" AND s.project = $%d", argN)
		args = append(args, project)
		argN++
	}

	query += fmt.Sprintf(" GROUP BY s.id ORDER BY MAX(COALESCE(o.created_at, s.started_at)) DESC LIMIT $%d", argN)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SessionSummary
	for rows.Next() {
		var ss SessionSummary
		var startedAt time.Time
		var endedAt *time.Time
		if err := rows.Scan(&ss.ID, &ss.Project, &startedAt, &endedAt, &ss.Summary, &ss.ObservationCount); err != nil {
			return nil, err
		}
		ss.StartedAt = formatTS(startedAt)
		ss.EndedAt = formatNullableTS(endedAt)
		results = append(results, ss)
	}
	return results, rows.Err()
}

func (s *PostgresStore) AllSessions(project string, limit int) ([]SessionSummary, error) {
	if limit <= 0 {
		limit = 50
	}
	ctx := context.Background()

	query := `
		SELECT s.id, s.project, s.started_at, s.ended_at, s.summary,
		       COUNT(o.id) as observation_count
		FROM sessions s
		LEFT JOIN observations o ON o.session_id = s.id AND o.deleted_at IS NULL
		WHERE TRUE
	`
	args := []any{}
	argN := 1

	if project != "" {
		query += fmt.Sprintf(" AND s.project = $%d", argN)
		args = append(args, project)
		argN++
	}

	query += fmt.Sprintf(" GROUP BY s.id ORDER BY MAX(COALESCE(o.created_at, s.started_at)) DESC LIMIT $%d", argN)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SessionSummary
	for rows.Next() {
		var ss SessionSummary
		var startedAt time.Time
		var endedAt *time.Time
		if err := rows.Scan(&ss.ID, &ss.Project, &startedAt, &endedAt, &ss.Summary, &ss.ObservationCount); err != nil {
			return nil, err
		}
		ss.StartedAt = formatTS(startedAt)
		ss.EndedAt = formatNullableTS(endedAt)
		results = append(results, ss)
	}
	return results, rows.Err()
}

// ─── Observations ────────────────────────────────────────────────────────────

func (s *PostgresStore) AddObservation(p AddObservationParams) (int64, error) {
	title := stripPrivateTags(p.Title)
	content := stripPrivateTags(p.Content)

	if len(content) > s.cfg.MaxObservationLength {
		content = content[:s.cfg.MaxObservationLength] + "... [truncated]"
	}
	scope := normalizeScope(p.Scope)
	normHash := hashNormalized(content)
	topicKey := normalizeTopicKey(p.TopicKey)

	ctx := context.Background()
	var observationID int64

	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var obs *Observation

		// ── Topic key upsert path ──
		if topicKey != "" {
			// Acquire an advisory lock scoped to this transaction to serialize
			// concurrent upserts on the same topic_key+project+scope combination.
			lockKey := topicKeyAdvisoryLock(topicKey, p.Project, scope)
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKey); err != nil {
				return fmt.Errorf("advisory lock for topic_key: %w", err)
			}

			var existingID int64
			err := tx.QueryRow(ctx,
				`SELECT id FROM observations
				 WHERE topic_key = $1
				   AND COALESCE(project, '') = COALESCE($2, '')
				   AND scope = $3
				   AND deleted_at IS NULL
				 ORDER BY updated_at DESC, created_at DESC
				 LIMIT 1`,
				topicKey, nullableString(p.Project), scope,
			).Scan(&existingID)
			if err == nil {
				if _, err := tx.Exec(ctx,
					`UPDATE observations
					 SET type = $1, title = $2, content = $3, tool_name = $4,
					     topic_key = $5, normalized_hash = $6,
					     revision_count = revision_count + 1,
					     last_seen_at = NOW(), updated_at = NOW()
					 WHERE id = $7`,
					p.Type, title, content, nullableString(p.ToolName),
					nullableString(topicKey), normHash, existingID,
				); err != nil {
					return err
				}
				obs, err = s.getObservationTx(ctx, tx, existingID)
				if err != nil {
					return err
				}
				observationID = existingID
				return s.enqueueSyncMutationTx(ctx, tx, SyncEntityObservation, obs.SyncID, SyncOpUpsert, observationPayloadFromObservation(obs))
			}
			if err != pgx.ErrNoRows {
				return err
			}
		}

		// ── Dedupe check ──
		dedupeInterval := dedupeWindowPG(s.cfg.DedupeWindow)
		var existingID int64
		err := tx.QueryRow(ctx,
			`SELECT id FROM observations
			 WHERE normalized_hash = $1
			   AND COALESCE(project, '') = COALESCE($2, '')
			   AND scope = $3
			   AND type = $4
			   AND title = $5
			   AND deleted_at IS NULL
			   AND created_at >= NOW() - $6::interval
			 ORDER BY created_at DESC
			 LIMIT 1`,
			normHash, nullableString(p.Project), scope, p.Type, title, dedupeInterval,
		).Scan(&existingID)
		if err == nil {
			if _, err := tx.Exec(ctx,
				`UPDATE observations
				 SET duplicate_count = duplicate_count + 1,
				     last_seen_at = NOW(), updated_at = NOW()
				 WHERE id = $1`,
				existingID,
			); err != nil {
				return err
			}
			obs, err = s.getObservationTx(ctx, tx, existingID)
			if err != nil {
				return err
			}
			observationID = existingID
			return s.enqueueSyncMutationTx(ctx, tx, SyncEntityObservation, obs.SyncID, SyncOpUpsert, observationPayloadFromObservation(obs))
		}
		if err != pgx.ErrNoRows {
			return err
		}

		// ── Fresh insert ──
		syncID := newSyncID("obs")
		if err := tx.QueryRow(ctx,
			`INSERT INTO observations (sync_id, session_id, type, title, content, tool_name, project, scope,
			   topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at, updated_at, created_by)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, 1, NOW(), NOW(), $11)
			 RETURNING id`,
			syncID, p.SessionID, p.Type, title, content,
			nullableString(p.ToolName), nullableString(p.Project), scope,
			nullableString(topicKey), normHash, s.identity,
		).Scan(&observationID); err != nil {
			return err
		}

		// Populate review_after for types that have a configured decay offset.
		// expires_at is intentionally NULL for all types in Phase 1.
		// This UPDATE runs only for NEW inserts (not topic_key revisions or deduplication).
		if months, ok := decayReviewAfterMonths[p.Type]; ok {
			reviewAfter := time.Now().UTC().AddDate(0, months, 0)
			if _, err := tx.Exec(ctx,
				`UPDATE observations SET review_after = $1 WHERE id = $2`,
				reviewAfter, observationID,
			); err != nil {
				return fmt.Errorf("set review_after: %w", err)
			}
		}

		obs, err = s.getObservationTx(ctx, tx, observationID)
		if err != nil {
			return err
		}
		return s.enqueueSyncMutationTx(ctx, tx, SyncEntityObservation, obs.SyncID, SyncOpUpsert, observationPayloadFromObservation(obs))
	})
	if err != nil {
		return 0, err
	}
	return observationID, nil
}

func (s *PostgresStore) GetObservation(id int64) (*Observation, error) {
	ctx := context.Background()
	return s.scanObservation(ctx, s.pool,
		`SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE id = $1 AND deleted_at IS NULL`, id)
}

func (s *PostgresStore) UpdateObservation(id int64, p UpdateObservationParams) (*Observation, error) {
	ctx := context.Background()
	var updated *Observation
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		obs, err := s.getObservationTx(ctx, tx, id)
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
			project = *p.Project
		}
		if p.Scope != nil {
			scope = normalizeScope(*p.Scope)
		}
		if p.TopicKey != nil {
			topicKey = normalizeTopicKey(*p.TopicKey)
		}

		if _, err := tx.Exec(ctx,
			`UPDATE observations
			 SET type = $1, title = $2, content = $3, project = $4, scope = $5,
			     topic_key = $6, normalized_hash = $7, revision_count = revision_count + 1,
			     updated_at = NOW()
			 WHERE id = $8 AND deleted_at IS NULL`,
			typ, title, content, nullableString(project), scope,
			nullableString(topicKey), hashNormalized(content), id,
		); err != nil {
			return err
		}

		updated, err = s.getObservationTx(ctx, tx, id)
		if err != nil {
			return err
		}
		return s.enqueueSyncMutationTx(ctx, tx, SyncEntityObservation, updated.SyncID, SyncOpUpsert, observationPayloadFromObservation(updated))
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *PostgresStore) DeleteObservation(id int64, hardDelete bool) error {
	ctx := context.Background()
	return s.withTx(ctx, func(tx pgx.Tx) error {
		obs, err := s.getObservationTx(ctx, tx, id)
		if err == pgx.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}

		deletedAt := Now()
		if hardDelete {
			if _, err := tx.Exec(ctx, `DELETE FROM observations WHERE id = $1`, id); err != nil {
				return err
			}
		} else {
			var deletedTime time.Time
			if err := tx.QueryRow(ctx,
				`UPDATE observations
				 SET deleted_at = NOW(), updated_at = NOW()
				 WHERE id = $1 AND deleted_at IS NULL
				 RETURNING deleted_at`,
				id,
			).Scan(&deletedTime); err != nil {
				return err
			}
			deletedAt = formatTS(deletedTime)
		}

		return s.enqueueSyncMutationTx(ctx, tx, SyncEntityObservation, obs.SyncID, SyncOpDelete, syncObservationPayload{
			SyncID:     obs.SyncID,
			Deleted:    true,
			DeletedAt:  &deletedAt,
			HardDelete: hardDelete,
		})
	})
}

func (s *PostgresStore) AllObservations(project, scope string, limit int) ([]Observation, error) {
	if limit <= 0 {
		limit = s.cfg.MaxContextResults
	}
	ctx := context.Background()

	query := `
		SELECT o.id, COALESCE(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
		       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.created_at, o.updated_at, o.deleted_at
		FROM observations o
		WHERE o.deleted_at IS NULL
	`
	args := []any{}
	argN := 1

	if project != "" {
		query += fmt.Sprintf(" AND o.project = $%d", argN)
		args = append(args, project)
		argN++
	}
	if scope != "" {
		query += fmt.Sprintf(" AND o.scope = $%d", argN)
		args = append(args, normalizeScope(scope))
		argN++
	}

	query += fmt.Sprintf(" ORDER BY o.created_at DESC LIMIT $%d", argN)
	args = append(args, limit)

	return s.queryObservationsPG(ctx, query, args...)
}

func (s *PostgresStore) SessionObservations(sessionID string, limit int) ([]Observation, error) {
	if limit <= 0 {
		limit = 200
	}
	ctx := context.Background()
	return s.queryObservationsPG(ctx,
		`SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations
		 WHERE session_id = $1 AND deleted_at IS NULL
		 ORDER BY created_at ASC
		 LIMIT $2`,
		sessionID, limit,
	)
}

func (s *PostgresStore) RecentObservations(project, scope string, limit int) ([]Observation, error) {
	if limit <= 0 {
		limit = s.cfg.MaxContextResults
	}
	ctx := context.Background()

	query := `
		SELECT o.id, COALESCE(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
		       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.created_at, o.updated_at, o.deleted_at
		FROM observations o
		WHERE o.deleted_at IS NULL
	`
	args := []any{}
	argN := 1

	if project != "" {
		query += fmt.Sprintf(" AND o.project = $%d", argN)
		args = append(args, project)
		argN++
	}
	if scope != "" {
		query += fmt.Sprintf(" AND o.scope = $%d", argN)
		args = append(args, normalizeScope(scope))
		argN++
	}

	query += fmt.Sprintf(" ORDER BY o.created_at DESC LIMIT $%d", argN)
	args = append(args, limit)

	return s.queryObservationsPG(ctx, query, args...)
}

func (s *PostgresStore) GetObservationBySyncID(syncID string) (*Observation, error) {
	ctx := context.Background()
	return s.scanObservation(ctx, s.pool,
		`SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE sync_id = $1 AND deleted_at IS NULL ORDER BY id DESC LIMIT 1`, syncID)
}

// ─── User Prompts ────────────────────────────────────────────────────────────

func (s *PostgresStore) AddPrompt(p AddPromptParams) (int64, error) {
	content := stripPrivateTags(p.Content)
	if len(content) > s.cfg.MaxObservationLength {
		content = content[:s.cfg.MaxObservationLength] + "... [truncated]"
	}

	ctx := context.Background()
	var promptID int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		syncID := newSyncID("prompt")
		if err := tx.QueryRow(ctx,
			`INSERT INTO user_prompts (sync_id, session_id, content, project, created_by) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			syncID, p.SessionID, content, nullableString(p.Project), s.identity,
		).Scan(&promptID); err != nil {
			return err
		}
		return s.enqueueSyncMutationTx(ctx, tx, SyncEntityPrompt, syncID, SyncOpUpsert, syncPromptPayload{
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

func (s *PostgresStore) RecentPrompts(project string, limit int) ([]Prompt, error) {
	if limit <= 0 {
		limit = 20
	}
	ctx := context.Background()

	query := `SELECT id, COALESCE(sync_id, '') as sync_id, session_id, content, COALESCE(project, '') as project, created_at FROM user_prompts`
	args := []any{}
	argN := 1

	if project != "" {
		query += fmt.Sprintf(" WHERE project = $%d", argN)
		args = append(args, project)
		argN++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", argN)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Prompt
	for rows.Next() {
		var p Prompt
		var createdAt time.Time
		if err := rows.Scan(&p.ID, &p.SyncID, &p.SessionID, &p.Content, &p.Project, &createdAt); err != nil {
			return nil, err
		}
		p.CreatedAt = formatTS(createdAt)
		results = append(results, p)
	}
	return results, rows.Err()
}

func (s *PostgresStore) SearchPrompts(query string, project string, limit int) ([]Prompt, error) {
	if limit <= 0 {
		limit = 10
	}
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}

	ctx := context.Background()
	lang := ftsLanguage()

	sqlQ := fmt.Sprintf(`
		SELECT p.id, COALESCE(p.sync_id, '') as sync_id, p.session_id, p.content,
		       COALESCE(p.project, '') as project, p.created_at
		FROM user_prompts p, websearch_to_tsquery('%s', $1) q
		WHERE p.search_vector @@ q
	`, lang)
	args := []any{query}
	argN := 2

	if project != "" {
		sqlQ += fmt.Sprintf(" AND p.project = $%d", argN)
		args = append(args, project)
		argN++
	}

	sqlQ += fmt.Sprintf(" ORDER BY ts_rank(p.search_vector, q) DESC LIMIT $%d", argN)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, sqlQ, args...)
	if err != nil {
		// Fallback to plainto_tsquery on error.
		sqlQ2 := fmt.Sprintf(`
			SELECT p.id, COALESCE(p.sync_id, '') as sync_id, p.session_id, p.content,
			       COALESCE(p.project, '') as project, p.created_at
			FROM user_prompts p, plainto_tsquery('%s', $1) q
			WHERE p.search_vector @@ q
		`, lang)
		args2 := []any{query}
		argN2 := 2
		if project != "" {
			sqlQ2 += fmt.Sprintf(" AND p.project = $%d", argN2)
			args2 = append(args2, project)
			argN2++
		}
		sqlQ2 += fmt.Sprintf(" ORDER BY ts_rank(p.search_vector, q) DESC LIMIT $%d", argN2)
		args2 = append(args2, limit)

		rows, err = s.pool.Query(ctx, sqlQ2, args2...)
		if err != nil {
			return nil, fmt.Errorf("search prompts: %w", err)
		}
	}
	defer rows.Close()

	var results []Prompt
	for rows.Next() {
		var p Prompt
		var createdAt time.Time
		if err := rows.Scan(&p.ID, &p.SyncID, &p.SessionID, &p.Content, &p.Project, &createdAt); err != nil {
			return nil, err
		}
		p.CreatedAt = formatTS(createdAt)
		results = append(results, p)
	}
	return results, rows.Err()
}

// ─── Search (tsvector) ───────────────────────────────────────────────────────

func (s *PostgresStore) Search(query string, opts SearchOptions) ([]SearchResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}
	if limit > s.cfg.MaxSearchResults {
		limit = s.cfg.MaxSearchResults
	}
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}

	ctx := context.Background()
	lang := ftsLanguage()

	// Resolve since filter once.
	sinceTS := ""
	if opts.Since != "" {
		sinceTS = resolveSince(opts.Since)
	}

	// ── Topic key direct lookup ──
	var directResults []SearchResult
	if strings.Contains(query, "/") {
		tkSQL := `
			SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
			       scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
			FROM observations
			WHERE topic_key = $1 AND deleted_at IS NULL
		`
		tkArgs := []any{query}
		argN := 2

		if opts.Type != "" {
			tkSQL += fmt.Sprintf(" AND type = $%d", argN)
			tkArgs = append(tkArgs, opts.Type)
			argN++
		}
		if opts.Project != "" {
			tkSQL += fmt.Sprintf(" AND project = $%d", argN)
			tkArgs = append(tkArgs, opts.Project)
			argN++
		}
		if opts.Scope != "" {
			tkSQL += fmt.Sprintf(" AND scope = $%d", argN)
			tkArgs = append(tkArgs, normalizeScope(opts.Scope))
			argN++
		}
		if opts.User != "" {
			tkSQL += fmt.Sprintf(" AND created_by = $%d", argN)
			tkArgs = append(tkArgs, opts.User)
			argN++
		}
		if sinceTS != "" {
			tkSQL += fmt.Sprintf(" AND created_at >= $%d", argN)
			tkArgs = append(tkArgs, sinceTS)
			argN++
		}
		tkSQL += fmt.Sprintf(" ORDER BY updated_at DESC LIMIT $%d", argN)
		tkArgs = append(tkArgs, limit)

		tkObs, err := s.queryObservationsPG(ctx, tkSQL, tkArgs...)
		if err == nil {
			for _, o := range tkObs {
				directResults = append(directResults, SearchResult{Observation: o, Rank: -1000})
			}
		}
	}

	// ── FTS search via tsvector ──
	ftsSQL := fmt.Sprintf(`
		SELECT o.id, COALESCE(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content,
		       o.tool_name, o.project, o.scope, o.topic_key, o.revision_count, o.duplicate_count,
		       o.last_seen_at, o.created_at, o.updated_at, o.deleted_at,
		       ts_rank(o.search_vector, q) AS rank
		FROM observations o, websearch_to_tsquery('%s', $1) q
		WHERE o.search_vector @@ q AND o.deleted_at IS NULL
	`, lang)
	args := []any{query}
	argN := 2

	if opts.Type != "" {
		ftsSQL += fmt.Sprintf(" AND o.type = $%d", argN)
		args = append(args, opts.Type)
		argN++
	}
	if opts.Project != "" {
		ftsSQL += fmt.Sprintf(" AND o.project = $%d", argN)
		args = append(args, opts.Project)
		argN++
	}
	if opts.Scope != "" {
		ftsSQL += fmt.Sprintf(" AND o.scope = $%d", argN)
		args = append(args, normalizeScope(opts.Scope))
		argN++
	}
	if opts.User != "" {
		ftsSQL += fmt.Sprintf(" AND o.created_by = $%d", argN)
		args = append(args, opts.User)
		argN++
	}
	if sinceTS != "" {
		ftsSQL += fmt.Sprintf(" AND o.created_at >= $%d", argN)
		args = append(args, sinceTS)
		argN++
	}
	ftsSQL += fmt.Sprintf(" ORDER BY rank DESC LIMIT $%d", argN)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, ftsSQL, args...)
	if err != nil {
		// Fallback to plainto_tsquery.
		ftsSQL2 := fmt.Sprintf(`
			SELECT o.id, COALESCE(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content,
			       o.tool_name, o.project, o.scope, o.topic_key, o.revision_count, o.duplicate_count,
			       o.last_seen_at, o.created_at, o.updated_at, o.deleted_at,
			       ts_rank(o.search_vector, q) AS rank
			FROM observations o, plainto_tsquery('%s', $1) q
			WHERE o.search_vector @@ q AND o.deleted_at IS NULL
		`, lang)
		args2 := []any{query}
		argN2 := 2
		if opts.Type != "" {
			ftsSQL2 += fmt.Sprintf(" AND o.type = $%d", argN2)
			args2 = append(args2, opts.Type)
			argN2++
		}
		if opts.Project != "" {
			ftsSQL2 += fmt.Sprintf(" AND o.project = $%d", argN2)
			args2 = append(args2, opts.Project)
			argN2++
		}
		if opts.Scope != "" {
			ftsSQL2 += fmt.Sprintf(" AND o.scope = $%d", argN2)
			args2 = append(args2, normalizeScope(opts.Scope))
			argN2++
		}
		if opts.User != "" {
			ftsSQL2 += fmt.Sprintf(" AND o.created_by = $%d", argN2)
			args2 = append(args2, opts.User)
			argN2++
		}
		if sinceTS != "" {
			ftsSQL2 += fmt.Sprintf(" AND o.created_at >= $%d", argN2)
			args2 = append(args2, sinceTS)
			argN2++
		}
		ftsSQL2 += fmt.Sprintf(" ORDER BY rank DESC LIMIT $%d", argN2)
		args2 = append(args2, limit)

		rows, err = s.pool.Query(ctx, ftsSQL2, args2...)
		if err != nil {
			return nil, fmt.Errorf("search: %w", err)
		}
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
		var createdAt, updatedAt time.Time
		var lastSeenAt, deletedAt *time.Time
		if err := rows.Scan(
			&sr.ID, &sr.SyncID, &sr.SessionID, &sr.Type, &sr.Title, &sr.Content,
			&sr.ToolName, &sr.Project, &sr.Scope, &sr.TopicKey, &sr.RevisionCount, &sr.DuplicateCount,
			&lastSeenAt, &createdAt, &updatedAt, &deletedAt,
			&sr.Rank,
		); err != nil {
			return nil, err
		}
		sr.CreatedAt = formatTS(createdAt)
		sr.UpdatedAt = formatTS(updatedAt)
		sr.LastSeenAt = formatNullableTS(lastSeenAt)
		sr.DeletedAt = formatNullableTS(deletedAt)
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

// ─── Timeline ────────────────────────────────────────────────────────────────

func (s *PostgresStore) Timeline(observationID int64, before, after int) (*TimelineResult, error) {
	if before <= 0 {
		before = 5
	}
	if after <= 0 {
		after = 5
	}
	ctx := context.Background()

	focus, err := s.GetObservation(observationID)
	if err != nil {
		return nil, fmt.Errorf("timeline: observation #%d not found: %w", observationID, err)
	}

	session, err := s.GetSession(focus.SessionID)
	if err != nil {
		session = nil
	}

	// Before entries.
	beforeRows, err := s.pool.Query(ctx, `
		SELECT id, session_id, type, title, content, tool_name, project,
		       scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		FROM observations
		WHERE session_id = $1 AND id < $2 AND deleted_at IS NULL
		ORDER BY id DESC
		LIMIT $3
	`, focus.SessionID, observationID, before)
	if err != nil {
		return nil, fmt.Errorf("timeline: before query: %w", err)
	}
	defer beforeRows.Close()

	var beforeEntries []TimelineEntry
	for beforeRows.Next() {
		e, err := scanTimelineEntry(beforeRows)
		if err != nil {
			return nil, err
		}
		beforeEntries = append(beforeEntries, e)
	}
	if err := beforeRows.Err(); err != nil {
		return nil, err
	}
	// Reverse to chronological order.
	for i, j := 0, len(beforeEntries)-1; i < j; i, j = i+1, j-1 {
		beforeEntries[i], beforeEntries[j] = beforeEntries[j], beforeEntries[i]
	}

	// After entries.
	afterRows, err := s.pool.Query(ctx, `
		SELECT id, session_id, type, title, content, tool_name, project,
		       scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		FROM observations
		WHERE session_id = $1 AND id > $2 AND deleted_at IS NULL
		ORDER BY id ASC
		LIMIT $3
	`, focus.SessionID, observationID, after)
	if err != nil {
		return nil, fmt.Errorf("timeline: after query: %w", err)
	}
	defer afterRows.Close()

	var afterEntries []TimelineEntry
	for afterRows.Next() {
		e, err := scanTimelineEntry(afterRows)
		if err != nil {
			return nil, err
		}
		afterEntries = append(afterEntries, e)
	}
	if err := afterRows.Err(); err != nil {
		return nil, err
	}

	var totalInRange int
	s.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM observations WHERE session_id = $1 AND deleted_at IS NULL", focus.SessionID,
	).Scan(&totalInRange)

	return &TimelineResult{
		Focus:        *focus,
		Before:       beforeEntries,
		After:        afterEntries,
		SessionInfo:  session,
		TotalInRange: totalInRange,
	}, nil
}

// ─── Stats ───────────────────────────────────────────────────────────────────

func (s *PostgresStore) Stats() (*Stats, error) {
	ctx := context.Background()
	stats := &Stats{}

	s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM sessions").Scan(&stats.TotalSessions)
	s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM observations WHERE deleted_at IS NULL").Scan(&stats.TotalObservations)
	s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM user_prompts").Scan(&stats.TotalPrompts)

	rows, err := s.pool.Query(ctx,
		"SELECT project FROM observations WHERE project IS NOT NULL AND deleted_at IS NULL GROUP BY project ORDER BY MAX(created_at) DESC")
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

func (s *PostgresStore) FormatContext(project, scope string) (string, error) {
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

func (s *PostgresStore) Export() (*ExportData, error) {
	ctx := context.Background()
	data := &ExportData{
		Version:    "0.1.0",
		ExportedAt: Now(),
	}

	// Sessions.
	sRows, err := s.pool.Query(ctx,
		"SELECT id, project, directory, started_at, ended_at, summary FROM sessions ORDER BY started_at")
	if err != nil {
		return nil, fmt.Errorf("export sessions: %w", err)
	}
	defer sRows.Close()
	for sRows.Next() {
		var sess Session
		var startedAt time.Time
		var endedAt *time.Time
		if err := sRows.Scan(&sess.ID, &sess.Project, &sess.Directory, &startedAt, &endedAt, &sess.Summary); err != nil {
			return nil, err
		}
		sess.StartedAt = formatTS(startedAt)
		sess.EndedAt = formatNullableTS(endedAt)
		data.Sessions = append(data.Sessions, sess)
	}
	if err := sRows.Err(); err != nil {
		return nil, err
	}

	// Observations.
	oRows, err := s.pool.Query(ctx,
		`SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("export observations: %w", err)
	}
	defer oRows.Close()
	for oRows.Next() {
		o, err := scanObservationRow(oRows)
		if err != nil {
			return nil, err
		}
		data.Observations = append(data.Observations, o)
	}
	if err := oRows.Err(); err != nil {
		return nil, err
	}

	// Prompts.
	pRows, err := s.pool.Query(ctx,
		"SELECT id, COALESCE(sync_id, '') as sync_id, session_id, content, COALESCE(project, '') as project, created_at FROM user_prompts ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("export prompts: %w", err)
	}
	defer pRows.Close()
	for pRows.Next() {
		var p Prompt
		var createdAt time.Time
		if err := pRows.Scan(&p.ID, &p.SyncID, &p.SessionID, &p.Content, &p.Project, &createdAt); err != nil {
			return nil, err
		}
		p.CreatedAt = formatTS(createdAt)
		data.Prompts = append(data.Prompts, p)
	}
	if err := pRows.Err(); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *PostgresStore) Import(data *ExportData) (*ImportResult, error) {
	ctx := context.Background()
	result := &ImportResult{}

	err := s.withTx(ctx, func(tx pgx.Tx) error {
		for _, sess := range data.Sessions {
			tag, err := tx.Exec(ctx,
				`INSERT INTO sessions (id, project, directory, started_at, ended_at, summary)
				 VALUES ($1, $2, $3, $4, $5, $6)
				 ON CONFLICT(id) DO NOTHING`,
				sess.ID, sess.Project, sess.Directory, sess.StartedAt, sess.EndedAt, sess.Summary,
			)
			if err != nil {
				return fmt.Errorf("import session %s: %w", sess.ID, err)
			}
			result.SessionsImported += int(tag.RowsAffected())
		}

		for _, obs := range data.Observations {
			tag, err := tx.Exec(ctx,
				`INSERT INTO observations (sync_id, session_id, type, title, content, tool_name, project, scope,
				   topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
				 ON CONFLICT(sync_id) DO NOTHING`,
				normalizeExistingSyncID(obs.SyncID, "obs"),
				obs.SessionID, obs.Type, obs.Title, obs.Content, obs.ToolName, obs.Project,
				normalizeScope(obs.Scope), nullableString(normalizeTopicKey(derefString(obs.TopicKey))),
				hashNormalized(obs.Content), maxInt(obs.RevisionCount, 1), maxInt(obs.DuplicateCount, 1),
				obs.LastSeenAt, obs.CreatedAt, obs.UpdatedAt, obs.DeletedAt,
			)
			if err != nil {
				return fmt.Errorf("import observation %d: %w", obs.ID, err)
			}
			result.ObservationsImported += int(tag.RowsAffected())
		}

		for _, p := range data.Prompts {
			tag, err := tx.Exec(ctx,
				`INSERT INTO user_prompts (sync_id, session_id, content, project, created_at)
				 VALUES ($1, $2, $3, $4, $5)
				 ON CONFLICT(sync_id) DO NOTHING`,
				normalizeExistingSyncID(p.SyncID, "prompt"), p.SessionID, p.Content, p.Project, p.CreatedAt,
			)
			if err != nil {
				return fmt.Errorf("import prompt %d: %w", p.ID, err)
			}
			result.PromptsImported += int(tag.RowsAffected())
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ─── Sync Chunk Tracking ─────────────────────────────────────────────────────

func (s *PostgresStore) GetSyncedChunks() (map[string]bool, error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx, "SELECT chunk_id FROM sync_chunks")
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

func (s *PostgresStore) RecordSyncedChunk(chunkID string) error {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx,
		"INSERT INTO sync_chunks (chunk_id) VALUES ($1) ON CONFLICT(chunk_id) DO NOTHING", chunkID)
	return err
}

// ─── Sync State ──────────────────────────────────────────────────────────────

func (s *PostgresStore) GetSyncState(targetKey string) (*SyncState, error) {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	if err := s.ensureSyncState(ctx, targetKey); err != nil {
		return nil, err
	}
	return s.getSyncState(ctx, targetKey)
}

func (s *PostgresStore) ListPendingSyncMutations(targetKey string, limit int) ([]SyncMutation, error) {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT sm.seq, sm.target_key, sm.entity, sm.entity_key, sm.op, sm.payload::text, sm.source, sm.project, sm.occurred_at, sm.acked_at
		FROM sync_mutations sm
		LEFT JOIN sync_enrolled_projects sep ON sm.project = sep.project
		WHERE sm.target_key = $1 AND sm.acked_at IS NULL
		  AND (sm.project = '' OR sep.project IS NOT NULL)
		ORDER BY sm.seq ASC
		LIMIT $2`, targetKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mutations []SyncMutation
	for rows.Next() {
		m, err := scanSyncMutation(rows)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, m)
	}
	return mutations, rows.Err()
}

func (s *PostgresStore) SkipAckNonEnrolledMutations(targetKey string) (int64, error) {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	tag, err := s.pool.Exec(ctx, `
		UPDATE sync_mutations
		SET acked_at = NOW()
		WHERE target_key = $1
		  AND acked_at IS NULL
		  AND project != ''
		  AND project NOT IN (SELECT project FROM sync_enrolled_projects)`,
		targetKey)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *PostgresStore) AckSyncMutations(targetKey string, lastAckedSeq int64) error {
	if lastAckedSeq <= 0 {
		return nil
	}
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	return s.withTx(ctx, func(tx pgx.Tx) error {
		state, err := s.getSyncStateTx(ctx, tx, targetKey)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sync_mutations SET acked_at = NOW() WHERE target_key = $1 AND seq <= $2 AND acked_at IS NULL`,
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
		_, err = tx.Exec(ctx,
			`UPDATE sync_state SET last_acked_seq = $1, lifecycle = $2, updated_at = NOW() WHERE target_key = $3`,
			acked, lifecycle, targetKey)
		return err
	})
}

func (s *PostgresStore) AckSyncMutationSeqs(targetKey string, seqs []int64) error {
	if len(seqs) == 0 {
		return nil
	}
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	return s.withTx(ctx, func(tx pgx.Tx) error {
		state, err := s.getSyncStateTx(ctx, tx, targetKey)
		if err != nil {
			return err
		}
		maxSeq := state.LastAckedSeq
		for _, seq := range seqs {
			if seq <= 0 {
				continue
			}
			if _, err := tx.Exec(ctx,
				`UPDATE sync_mutations SET acked_at = NOW() WHERE target_key = $1 AND seq = $2 AND acked_at IS NULL`,
				targetKey, seq,
			); err != nil {
				return err
			}
			if seq > maxSeq {
				maxSeq = seq
			}
		}
		var remaining int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM sync_mutations WHERE target_key = $1 AND acked_at IS NULL`, targetKey,
		).Scan(&remaining); err != nil {
			return err
		}
		lifecycle := SyncLifecyclePending
		if remaining == 0 {
			lifecycle = SyncLifecycleHealthy
		}
		_, err = tx.Exec(ctx,
			`UPDATE sync_state SET last_acked_seq = $1, lifecycle = $2, updated_at = NOW() WHERE target_key = $3`,
			maxSeq, lifecycle, targetKey)
		return err
	})
}

func (s *PostgresStore) AcquireSyncLease(targetKey, owner string, ttl time.Duration, now time.Time) (bool, error) {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	if ttl <= 0 {
		ttl = time.Minute
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	var acquired bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		state, err := s.getSyncStateTx(ctx, tx, targetKey)
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
		_, err = tx.Exec(ctx,
			`UPDATE sync_state SET lease_owner = $1, lease_until = $2, updated_at = NOW() WHERE target_key = $3`,
			owner, leaseUntil, targetKey)
		if err == nil {
			acquired = true
		}
		return err
	})
	return acquired, err
}

func (s *PostgresStore) ReleaseSyncLease(targetKey, owner string) error {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	_, err := s.pool.Exec(ctx,
		`UPDATE sync_state SET lease_owner = NULL, lease_until = NULL, updated_at = NOW()
		 WHERE target_key = $1 AND (lease_owner = $2 OR lease_owner IS NULL OR lease_owner = '')`,
		targetKey, owner)
	return err
}

func (s *PostgresStore) MarkSyncFailure(targetKey, message string, backoffUntil time.Time) error {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	backoff := backoffUntil.UTC().Format(time.RFC3339)
	return s.withTx(ctx, func(tx pgx.Tx) error {
		state, err := s.getSyncStateTx(ctx, tx, targetKey)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE sync_state
			 SET lifecycle = $1, consecutive_failures = $2, backoff_until = $3, last_error = $4, updated_at = NOW()
			 WHERE target_key = $5`,
			SyncLifecycleDegraded, state.ConsecutiveFailures+1, backoff, message, targetKey)
		return err
	})
}

func (s *PostgresStore) MarkSyncHealthy(targetKey string) error {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	_, err := s.pool.Exec(ctx,
		`UPDATE sync_state
		 SET lifecycle = $1, consecutive_failures = 0, backoff_until = NULL, last_error = NULL, updated_at = NOW()
		 WHERE target_key = $2`,
		SyncLifecycleHealthy, targetKey)
	return err
}

func (s *PostgresStore) ApplyPulledMutation(targetKey string, mutation SyncMutation) error {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	return s.withTx(ctx, func(tx pgx.Tx) error {
		state, err := s.getSyncStateTx(ctx, tx, targetKey)
		if err != nil {
			return err
		}
		if mutation.Seq <= state.LastPulledSeq {
			return nil
		}

		switch mutation.Entity {
		case SyncEntitySession:
			var payload syncSessionPayload
			if err := decodeSyncPayload([]byte(mutation.Payload), &payload); err != nil {
				return err
			}
			if err := s.applySessionPayloadTx(ctx, tx, payload); err != nil {
				return err
			}
		case SyncEntityObservation:
			var payload syncObservationPayload
			if err := decodeSyncPayload([]byte(mutation.Payload), &payload); err != nil {
				return err
			}
			if mutation.Op == SyncOpDelete {
				if err := s.applyObservationDeleteTx(ctx, tx, payload); err != nil {
					return err
				}
			} else {
				if err := s.applyObservationUpsertTx(ctx, tx, payload); err != nil {
					return err
				}
			}
		case SyncEntityPrompt:
			var payload syncPromptPayload
			if err := decodeSyncPayload([]byte(mutation.Payload), &payload); err != nil {
				return err
			}
			if err := s.applyPromptUpsertTx(ctx, tx, payload); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown sync entity %q", mutation.Entity)
		}

		_, err = tx.Exec(ctx,
			`UPDATE sync_state
			 SET last_pulled_seq = $1, lifecycle = $2, consecutive_failures = 0, backoff_until = NULL, last_error = NULL, updated_at = NOW()
			 WHERE target_key = $3`,
			mutation.Seq, SyncLifecycleHealthy, targetKey)
		return err
	})
}

// ─── Project Enrollment ──────────────────────────────────────────────────────

func (s *PostgresStore) EnrollProject(project string) error {
	if project == "" {
		return fmt.Errorf("project name must not be empty")
	}
	ctx := context.Background()
	return s.withTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO sync_enrolled_projects (project) VALUES ($1) ON CONFLICT(project) DO NOTHING`, project)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		return s.backfillProjectSyncMutationsTx(ctx, tx, project)
	})
}

func (s *PostgresStore) UnenrollProject(project string) error {
	if project == "" {
		return fmt.Errorf("project name must not be empty")
	}
	ctx := context.Background()
	_, err := s.pool.Exec(ctx,
		`DELETE FROM sync_enrolled_projects WHERE project = $1`, project)
	return err
}

func (s *PostgresStore) ListEnrolledProjects() ([]EnrolledProject, error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx,
		`SELECT project, enrolled_at FROM sync_enrolled_projects ORDER BY project ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []EnrolledProject
	for rows.Next() {
		var ep EnrolledProject
		var enrolledAt time.Time
		if err := rows.Scan(&ep.Project, &enrolledAt); err != nil {
			return nil, err
		}
		ep.EnrolledAt = formatTS(enrolledAt)
		projects = append(projects, ep)
	}
	return projects, rows.Err()
}

func (s *PostgresStore) IsProjectEnrolled(project string) (bool, error) {
	ctx := context.Background()
	var exists int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM sync_enrolled_projects WHERE project = $1 LIMIT 1`, project,
	).Scan(&exists)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ─── Project Migration ───────────────────────────────────────────────────────

func (s *PostgresStore) MigrateProject(oldName, newName string) (*MigrateResult, error) {
	if oldName == "" || newName == "" || oldName == newName {
		return &MigrateResult{}, nil
	}
	ctx := context.Background()

	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM observations WHERE project = $1
			UNION ALL
			SELECT 1 FROM sessions WHERE project = $1
			UNION ALL
			SELECT 1 FROM user_prompts WHERE project = $1
		)`, oldName,
	).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("check old project: %w", err)
	}
	if !exists {
		return &MigrateResult{}, nil
	}

	result := &MigrateResult{Migrated: true}
	err = s.withTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE observations SET project = $1 WHERE project = $2`, newName, oldName)
		if err != nil {
			return fmt.Errorf("migrate observations: %w", err)
		}
		result.ObservationsUpdated = tag.RowsAffected()

		tag, err = tx.Exec(ctx, `UPDATE sessions SET project = $1 WHERE project = $2`, newName, oldName)
		if err != nil {
			return fmt.Errorf("migrate sessions: %w", err)
		}
		result.SessionsUpdated = tag.RowsAffected()

		tag, err = tx.Exec(ctx, `UPDATE user_prompts SET project = $1 WHERE project = $2`, newName, oldName)
		if err != nil {
			return fmt.Errorf("migrate prompts: %w", err)
		}
		result.PromptsUpdated = tag.RowsAffected()

		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ─── Projects ────────────────────────────────────────────────────────────────

// ListProjects returns all projects with observation counts, contributor counts,
// and last activity date, ordered by most recently active first.
// When includeDeprecated is false, deprecated projects are excluded.
func (s *PostgresStore) ListProjects(includeDeprecated bool) ([]ProjectStats, error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx, `
		SELECT o.project,
		       COUNT(*) AS observations,
		       COUNT(DISTINCT o.created_by) AS contributors,
		       MAX(o.updated_at) AS last_activity,
		       COALESCE(pm.deprecated, false) AS deprecated
		FROM observations o
		LEFT JOIN project_metadata pm ON pm.project = o.project
		WHERE o.deleted_at IS NULL
		  AND o.project IS NOT NULL
		  AND o.project != ''
		  AND ($1 = true OR pm.deprecated IS NULL OR pm.deprecated = false)
		GROUP BY o.project, pm.deprecated
		ORDER BY last_activity DESC`, includeDeprecated)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()

	var results []ProjectStats
	for rows.Next() {
		var ps ProjectStats
		var lastActivity time.Time
		if err := rows.Scan(&ps.Project, &ps.Observations, &ps.Contributors, &lastActivity, &ps.Deprecated); err != nil {
			return nil, err
		}
		ps.LastActivity = formatTS(lastActivity)
		results = append(results, ps)
	}
	return results, rows.Err()
}

// DeprecateProject marks a project as deprecated in project_metadata.
// It upserts the row setting deprecated=true, deprecated_at=NOW(), deprecated_by=identity.
func (s *PostgresStore) DeprecateProject(project, identity string) error {
	if project == "" {
		return fmt.Errorf("project name must not be empty")
	}
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO project_metadata (project, deprecated, deprecated_at, deprecated_by, updated_at)
		VALUES ($1, true, NOW(), $2, NOW())
		ON CONFLICT (project) DO UPDATE
		  SET deprecated    = true,
		      deprecated_at = NOW(),
		      deprecated_by = $2,
		      updated_at    = NOW()`,
		project, identity)
	if err != nil {
		return fmt.Errorf("deprecate project: %w", err)
	}
	return nil
}

// ActivateProject removes the deprecated flag from a project.
// If the row doesn't exist, this is a no-op (project is active by default).
func (s *PostgresStore) ActivateProject(project string) error {
	if project == "" {
		return fmt.Errorf("project name must not be empty")
	}
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
		UPDATE project_metadata
		SET deprecated    = false,
		    deprecated_at = NULL,
		    deprecated_by = NULL,
		    updated_at    = NOW()
		WHERE project = $1`,
		project)
	if err != nil {
		return fmt.Errorf("activate project: %w", err)
	}
	return nil
}

// IsProjectDeprecated returns true if the project exists in project_metadata with deprecated=true.
func (s *PostgresStore) IsProjectDeprecated(project string) (bool, error) {
	if project == "" {
		return false, nil
	}
	ctx := context.Background()
	var deprecated bool
	err := s.pool.QueryRow(ctx, `
		SELECT deprecated FROM project_metadata WHERE project = $1`, project,
	).Scan(&deprecated)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("is project deprecated: %w", err)
	}
	return deprecated, nil
}

// PromoteObservation changes an observation's scope from 'personal' to 'project'.
// It validates that the observation exists, is personal, and is owned by identity.
// This operation is irreversible by design.
func (s *PostgresStore) PromoteObservation(id int64, identity string) error {
	ctx := context.Background()
	tag, err := s.pool.Exec(ctx,
		`UPDATE observations
		 SET scope = 'project', updated_at = NOW(), revision_count = revision_count + 1
		 WHERE id = $1 AND scope = 'personal' AND created_by = $2 AND deleted_at IS NULL`,
		id, identity,
	)
	if err != nil {
		return fmt.Errorf("promote observation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("observation not found, not personal scope, or not owned by you")
	}
	return nil
}

// ListContributors returns contributor activity stats, optionally filtered by project.
func (s *PostgresStore) ListContributors(project string) ([]ContributorStats, error) {
	ctx := context.Background()

	// Build the observations aggregation.
	obsQuery := `
		SELECT created_by,
		       COUNT(*) AS obs_count,
		       MAX(updated_at) AS last_obs,
		       array_agg(DISTINCT type ORDER BY type) AS types
		FROM observations
		WHERE deleted_at IS NULL
		  AND created_by IS NOT NULL
		  AND created_by != ''`
	obsArgs := []any{}
	argN := 1
	if project != "" {
		obsQuery += fmt.Sprintf(" AND project = $%d", argN)
		obsArgs = append(obsArgs, project)
		argN++
	}
	obsQuery += " GROUP BY created_by"

	obsRows, err := s.pool.Query(ctx, obsQuery, obsArgs...)
	if err != nil {
		return nil, fmt.Errorf("list contributors observations: %w", err)
	}
	defer obsRows.Close()

	type obsAgg struct {
		obsCount   int
		lastActive time.Time
		types      []string
	}
	obsMap := make(map[string]obsAgg)
	for obsRows.Next() {
		var identity string
		var count int
		var lastObs time.Time
		var types []string
		if err := obsRows.Scan(&identity, &count, &lastObs, &types); err != nil {
			return nil, err
		}
		obsMap[identity] = obsAgg{obsCount: count, lastActive: lastObs, types: types}
	}
	if err := obsRows.Err(); err != nil {
		return nil, err
	}

	// Build the prompts aggregation.
	pQuery := `
		SELECT created_by,
		       COUNT(*) AS prompt_count,
		       MAX(created_at) AS last_prompt
		FROM user_prompts
		WHERE created_by IS NOT NULL
		  AND created_by != ''`
	pArgs := []any{}
	pArgN := 1
	if project != "" {
		pQuery += fmt.Sprintf(" AND project = $%d", pArgN)
		pArgs = append(pArgs, project)
		pArgN++
	}
	pQuery += " GROUP BY created_by"

	pRows, err := s.pool.Query(ctx, pQuery, pArgs...)
	if err != nil {
		return nil, fmt.Errorf("list contributors prompts: %w", err)
	}
	defer pRows.Close()

	type promptAgg struct {
		count      int
		lastActive time.Time
	}
	promptMap := make(map[string]promptAgg)
	for pRows.Next() {
		var identity string
		var count int
		var lastPrompt time.Time
		if err := pRows.Scan(&identity, &count, &lastPrompt); err != nil {
			return nil, err
		}
		promptMap[identity] = promptAgg{count: count, lastActive: lastPrompt}
	}
	if err := pRows.Err(); err != nil {
		return nil, err
	}

	// Merge results — build a union of identities from both maps.
	seen := make(map[string]bool)
	var identities []string
	for id := range obsMap {
		if !seen[id] {
			seen[id] = true
			identities = append(identities, id)
		}
	}
	for id := range promptMap {
		if !seen[id] {
			seen[id] = true
			identities = append(identities, id)
		}
	}

	var results []ContributorStats
	for _, identity := range identities {
		cs := ContributorStats{Identity: identity}
		var lastActive time.Time
		if oa, ok := obsMap[identity]; ok {
			cs.Observations = oa.obsCount
			if oa.lastActive.After(lastActive) {
				lastActive = oa.lastActive
			}
			// Limit top types to first 3.
			types := oa.types
			if len(types) > 3 {
				types = types[:3]
			}
			cs.TopTypes = types
		}
		if pa, ok := promptMap[identity]; ok {
			cs.Prompts = pa.count
			if pa.lastActive.After(lastActive) {
				lastActive = pa.lastActive
			}
		}
		if !lastActive.IsZero() {
			cs.LastActive = formatTS(lastActive)
		}
		results = append(results, cs)
	}

	// Sort by last active descending.
	for i := 1; i < len(results); i++ {
		for j := i; j > 0 && results[j].LastActive > results[j-1].LastActive; j-- {
			results[j], results[j-1] = results[j-1], results[j]
		}
	}

	return results, nil
}

// ─── Delete Session ──────────────────────────────────────────────────────────

// DeleteSession hard-deletes a session and its prompts.
// Returns ErrSessionHasObservations if the session has any observations,
// and ErrSessionNotFound if no session with that ID exists.
func (s *PostgresStore) DeleteSession(id string) error {
	ctx := context.Background()
	return s.withTx(ctx, func(tx pgx.Tx) error {
		// Count ALL observations for the session (including soft-deleted).
		var count int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM observations WHERE session_id = $1`, id).Scan(&count); err != nil {
			return fmt.Errorf("delete session: count observations: %w", err)
		}
		if count > 0 {
			return fmt.Errorf("%w: session %q has %d observation(s)", ErrSessionHasObservations, id, count)
		}

		if _, err := tx.Exec(ctx, `DELETE FROM user_prompts WHERE session_id = $1`, id); err != nil {
			return fmt.Errorf("delete session: remove prompts: %w", err)
		}

		tag, err := tx.Exec(ctx, `DELETE FROM sessions WHERE id = $1`, id)
		if err != nil {
			return fmt.Errorf("delete session: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: %q", ErrSessionNotFound, id)
		}
		return nil
	})
}

// ─── Delete Prompt ───────────────────────────────────────────────────────────

// DeletePrompt hard-deletes a single prompt by ID.
// Returns ErrPromptNotFound if no prompt with that ID exists.
func (s *PostgresStore) DeletePrompt(id int64) error {
	ctx := context.Background()
	return s.withTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM user_prompts WHERE id = $1`, id)
		if err != nil {
			return fmt.Errorf("delete prompt: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: prompt #%d", ErrPromptNotFound, id)
		}
		return nil
	})
}

// ─── Project Queries ──────────────────────────────────────────────────────────

// ListProjectNames returns all distinct project names from observations,
// ordered alphabetically. Used for fuzzy matching and consolidation.
// ProjectExists returns true if the named project has at least one record in
// any of observations, sessions, prompts, or enrollment tables.
func (s *PostgresStore) ProjectExists(name string) (bool, error) {
	ctx := context.Background()
	const query = `
SELECT 1 FROM (
  SELECT project FROM observations WHERE project = $1 AND deleted_at IS NULL
  UNION ALL
  SELECT project FROM sessions WHERE project = $2
  UNION ALL
  SELECT project FROM user_prompts WHERE project = $3
  UNION ALL
  SELECT project FROM sync_enrolled_projects WHERE project = $4
) sub LIMIT 1`
	var dummy int
	err := s.pool.QueryRow(ctx, query, name, name, name, name).Scan(&dummy)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) ListProjectNames() ([]string, error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx,
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

// CountObservationsForProject returns the number of non-deleted observations
// for the given project name.
func (s *PostgresStore) CountObservationsForProject(name string) (int, error) {
	ctx := context.Background()
	var count int
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM observations WHERE project = $1 AND deleted_at IS NULL`, name,
	).Scan(&count)
	return count, err
}

// ListProjectsWithStats returns all projects with aggregated counts.
// Ordered by observation count descending.
func (s *PostgresStore) ListProjectsWithStats() ([]ProjectDetailStats, error) {
	ctx := context.Background()

	// Observation counts per project.
	obsRows, err := s.pool.Query(ctx,
		`SELECT project, COUNT(*) AS cnt
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

	// Session counts + directories per project.
	sessRows, err := s.pool.Query(ctx,
		`SELECT project, COUNT(*) AS cnt, directory
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

	// Prompt counts per project.
	promptRows, err := s.pool.Query(ctx,
		`SELECT project, COUNT(*) AS cnt
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

	// Deprecated status per project.
	deprRows, err := s.pool.Query(ctx,
		`SELECT project, deprecated FROM project_metadata WHERE deprecated = true`,
	)
	if err == nil {
		defer deprRows.Close()
		for deprRows.Next() {
			var name string
			var deprecated bool
			if scanErr := deprRows.Scan(&name, &deprecated); scanErr == nil {
				if statsMap[name] != nil {
					statsMap[name].Deprecated = deprecated
				}
			}
		}
	}

	// Convert to slice, sorted by observation count descending.
	results := make([]ProjectDetailStats, 0, len(statsMap))
	for _, ps := range statsMap {
		results = append(results, *ps)
	}
	for i := 1; i < len(results); i++ {
		for j := i; j > 0 && results[j].ObservationCount > results[j-1].ObservationCount; j-- {
			results[j], results[j-1] = results[j-1], results[j]
		}
	}
	return results, nil
}

// PruneProject removes all sessions and prompts for a project that has zero
// (non-deleted) observations. Returns an error if the project still has
// observations — the caller must verify first.
func (s *PostgresStore) PruneProject(project string) (*PruneResult, error) {
	if project == "" {
		return nil, fmt.Errorf("project name must not be empty")
	}

	count, err := s.CountObservationsForProject(project)
	if err != nil {
		return nil, fmt.Errorf("count observations: %w", err)
	}
	if count > 0 {
		return nil, fmt.Errorf("project %q still has %d observations — cannot prune", project, count)
	}

	ctx := context.Background()
	result := &PruneResult{Project: project}

	err = s.withTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM user_prompts WHERE project = $1`, project)
		if err != nil {
			return fmt.Errorf("prune prompts: %w", err)
		}
		result.PromptsDeleted = tag.RowsAffected()

		tag, err = tx.Exec(ctx, `DELETE FROM sessions WHERE project = $1`, project)
		if err != nil {
			return fmt.Errorf("prune sessions: %w", err)
		}
		result.SessionsDeleted = tag.RowsAffected()

		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MergeProjects migrates all records from each source project name into the
// canonical name. Sources that equal the canonical (after normalization) or
// have no records are silently skipped — the operation is idempotent.
func (s *PostgresStore) MergeProjects(sources []string, canonical string) (*MergeResult, error) {
	canonical, _ = NormalizeProject(canonical)
	if canonical == "" {
		return nil, fmt.Errorf("canonical project name must not be empty")
	}

	ctx := context.Background()
	result := &MergeResult{Canonical: canonical}

	err := s.withTx(ctx, func(tx pgx.Tx) error {
		for _, src := range sources {
			src, _ = NormalizeProject(src)
			if src == "" || src == canonical {
				continue
			}

			tag, err := tx.Exec(ctx, `UPDATE observations SET project = $1 WHERE project = $2`, canonical, src)
			if err != nil {
				return fmt.Errorf("merge observations %q → %q: %w", src, canonical, err)
			}
			result.ObservationsUpdated += tag.RowsAffected()

			tag, err = tx.Exec(ctx, `UPDATE sessions SET project = $1 WHERE project = $2`, canonical, src)
			if err != nil {
				return fmt.Errorf("merge sessions %q → %q: %w", src, canonical, err)
			}
			result.SessionsUpdated += tag.RowsAffected()

			tag, err = tx.Exec(ctx, `UPDATE user_prompts SET project = $1 WHERE project = $2`, canonical, src)
			if err != nil {
				return fmt.Errorf("merge prompts %q → %q: %w", src, canonical, err)
			}
			result.PromptsUpdated += tag.RowsAffected()

			result.SourcesMerged = append(result.SourcesMerged, src)
		}
		// Enqueue sync mutations so cloud sync picks up the merged records.
		return s.backfillProjectSyncMutationsTx(ctx, tx, canonical)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ─── Passive Capture ─────────────────────────────────────────────────────────

func (s *PostgresStore) PassiveCapture(p PassiveCaptureParams) (*PassiveCaptureResult, error) {
	result := &PassiveCaptureResult{}
	learnings := ExtractLearnings(p.Content)
	result.Extracted = len(learnings)

	if len(learnings) == 0 {
		return result, nil
	}

	ctx := context.Background()
	for _, learning := range learnings {
		normHash := hashNormalized(learning)
		var existingID int64
		err := s.pool.QueryRow(ctx,
			`SELECT id FROM observations
			 WHERE normalized_hash = $1
			   AND COALESCE(project, '') = COALESCE($2, '')
			   AND deleted_at IS NULL
			 LIMIT 1`,
			normHash, nullableString(p.Project),
		).Scan(&existingID)

		if err == nil {
			result.Duplicates++
			continue
		}

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

// ─── Private Helpers ─────────────────────────────────────────────────────────

// scanObservation scans a single observation row from a pgx.Row or pgx.Rows.
type pgQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *PostgresStore) scanObservation(ctx context.Context, q pgQuerier, sql string, args ...any) (*Observation, error) {
	row := q.QueryRow(ctx, sql, args...)
	var o Observation
	var createdAt, updatedAt time.Time
	var lastSeenAt, deletedAt *time.Time
	if err := row.Scan(
		&o.ID, &o.SyncID, &o.SessionID, &o.Type, &o.Title, &o.Content,
		&o.ToolName, &o.Project, &o.Scope, &o.TopicKey, &o.RevisionCount, &o.DuplicateCount,
		&lastSeenAt, &createdAt, &updatedAt, &deletedAt,
	); err != nil {
		return nil, err
	}
	o.CreatedAt = formatTS(createdAt)
	o.UpdatedAt = formatTS(updatedAt)
	o.LastSeenAt = formatNullableTS(lastSeenAt)
	o.DeletedAt = formatNullableTS(deletedAt)
	return &o, nil
}

func (s *PostgresStore) getObservationTx(ctx context.Context, tx pgx.Tx, id int64) (*Observation, error) {
	return s.scanObservation(ctx, tx,
		`SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE id = $1 AND deleted_at IS NULL`, id)
}

func (s *PostgresStore) getObservationBySyncIDTx(ctx context.Context, tx pgx.Tx, syncID string, includeDeleted bool) (*Observation, error) {
	query := `SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE sync_id = $1`
	if !includeDeleted {
		query += ` AND deleted_at IS NULL`
	}
	query += ` ORDER BY id DESC LIMIT 1`
	return s.scanObservation(ctx, tx, query, syncID)
}

func scanObservationRow(rows pgx.Rows) (Observation, error) {
	var o Observation
	var createdAt, updatedAt time.Time
	var lastSeenAt, deletedAt *time.Time
	if err := rows.Scan(
		&o.ID, &o.SyncID, &o.SessionID, &o.Type, &o.Title, &o.Content,
		&o.ToolName, &o.Project, &o.Scope, &o.TopicKey, &o.RevisionCount, &o.DuplicateCount,
		&lastSeenAt, &createdAt, &updatedAt, &deletedAt,
	); err != nil {
		return o, err
	}
	o.CreatedAt = formatTS(createdAt)
	o.UpdatedAt = formatTS(updatedAt)
	o.LastSeenAt = formatNullableTS(lastSeenAt)
	o.DeletedAt = formatNullableTS(deletedAt)
	return o, nil
}

func (s *PostgresStore) queryObservationsPG(ctx context.Context, query string, args ...any) ([]Observation, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Observation
	for rows.Next() {
		o, err := scanObservationRow(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, o)
	}
	return results, rows.Err()
}

func scanTimelineEntry(rows pgx.Rows) (TimelineEntry, error) {
	var e TimelineEntry
	var createdAt, updatedAt time.Time
	var lastSeenAt, deletedAt *time.Time
	if err := rows.Scan(
		&e.ID, &e.SessionID, &e.Type, &e.Title, &e.Content,
		&e.ToolName, &e.Project, &e.Scope, &e.TopicKey, &e.RevisionCount, &e.DuplicateCount,
		&lastSeenAt, &createdAt, &updatedAt, &deletedAt,
	); err != nil {
		return e, err
	}
	e.CreatedAt = formatTS(createdAt)
	e.UpdatedAt = formatTS(updatedAt)
	e.LastSeenAt = formatNullableTS(lastSeenAt)
	e.DeletedAt = formatNullableTS(deletedAt)
	return e, nil
}

func scanSyncMutation(rows pgx.Rows) (SyncMutation, error) {
	var m SyncMutation
	var occurredAt time.Time
	var ackedAt *time.Time
	if err := rows.Scan(&m.Seq, &m.TargetKey, &m.Entity, &m.EntityKey, &m.Op, &m.Payload, &m.Source, &m.Project, &occurredAt, &ackedAt); err != nil {
		return m, err
	}
	m.OccurredAt = formatTS(occurredAt)
	if ackedAt != nil {
		s := formatTS(*ackedAt)
		m.AckedAt = &s
	}
	return m, nil
}

func (s *PostgresStore) ensureSyncState(ctx context.Context, targetKey string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO sync_state (target_key, lifecycle, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT(target_key) DO NOTHING`,
		targetKey, SyncLifecycleIdle)
	return err
}

func (s *PostgresStore) getSyncState(ctx context.Context, targetKey string) (*SyncState, error) {
	var state SyncState
	var updatedAt time.Time
	var backoffUntil, leaseUntil *time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT target_key, lifecycle, last_enqueued_seq, last_acked_seq, last_pulled_seq,
		       consecutive_failures, backoff_until, lease_owner, lease_until, last_error, updated_at
		FROM sync_state WHERE target_key = $1`, targetKey,
	).Scan(&state.TargetKey, &state.Lifecycle, &state.LastEnqueuedSeq, &state.LastAckedSeq, &state.LastPulledSeq,
		&state.ConsecutiveFailures, &backoffUntil, &state.LeaseOwner, &leaseUntil, &state.LastError, &updatedAt,
	); err != nil {
		return nil, err
	}
	state.UpdatedAt = formatTS(updatedAt)
	if backoffUntil != nil {
		s := backoffUntil.UTC().Format(time.RFC3339)
		state.BackoffUntil = &s
	}
	if leaseUntil != nil {
		s := leaseUntil.UTC().Format(time.RFC3339)
		state.LeaseUntil = &s
	}
	return &state, nil
}

func (s *PostgresStore) getSyncStateTx(ctx context.Context, tx pgx.Tx, targetKey string) (*SyncState, error) {
	if _, err := tx.Exec(ctx,
		`INSERT INTO sync_state (target_key, lifecycle, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT(target_key) DO NOTHING`,
		targetKey, SyncLifecycleIdle,
	); err != nil {
		return nil, err
	}
	var state SyncState
	var updatedAt time.Time
	var backoffUntil, leaseUntil *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT target_key, lifecycle, last_enqueued_seq, last_acked_seq, last_pulled_seq,
		       consecutive_failures, backoff_until, lease_owner, lease_until, last_error, updated_at
		FROM sync_state WHERE target_key = $1`, targetKey,
	).Scan(&state.TargetKey, &state.Lifecycle, &state.LastEnqueuedSeq, &state.LastAckedSeq, &state.LastPulledSeq,
		&state.ConsecutiveFailures, &backoffUntil, &state.LeaseOwner, &leaseUntil, &state.LastError, &updatedAt,
	); err != nil {
		return nil, err
	}
	state.UpdatedAt = formatTS(updatedAt)
	if backoffUntil != nil {
		s := backoffUntil.UTC().Format(time.RFC3339)
		state.BackoffUntil = &s
	}
	if leaseUntil != nil {
		s := leaseUntil.UTC().Format(time.RFC3339)
		state.LeaseUntil = &s
	}
	return &state, nil
}

func (s *PostgresStore) enqueueSyncMutationTx(ctx context.Context, tx pgx.Tx, entity, entityKey, op string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	project := extractProjectFromPayload(payload)
	if _, err := tx.Exec(ctx,
		`INSERT INTO sync_state (target_key, lifecycle, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT(target_key) DO NOTHING`,
		DefaultSyncTargetKey, SyncLifecycleIdle,
	); err != nil {
		return err
	}
	var seq int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO sync_mutations (target_key, entity, entity_key, op, payload, source, project)
		 VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
		 RETURNING seq`,
		DefaultSyncTargetKey, entity, entityKey, op, string(encoded), SyncSourceLocal, project,
	).Scan(&seq); err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`UPDATE sync_state SET lifecycle = $1, last_enqueued_seq = $2, updated_at = NOW() WHERE target_key = $3`,
		SyncLifecyclePending, seq, DefaultSyncTargetKey)
	return err
}

func (s *PostgresStore) applySessionPayloadTx(ctx context.Context, tx pgx.Tx, payload syncSessionPayload) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO sessions (id, project, directory, ended_at, summary)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT(id) DO UPDATE SET
		   project = EXCLUDED.project,
		   directory = EXCLUDED.directory,
		   ended_at = COALESCE(EXCLUDED.ended_at, sessions.ended_at),
		   summary = COALESCE(EXCLUDED.summary, sessions.summary)`,
		payload.ID, payload.Project, payload.Directory, payload.EndedAt, payload.Summary)
	return err
}

func (s *PostgresStore) applyObservationUpsertTx(ctx context.Context, tx pgx.Tx, payload syncObservationPayload) error {
	existing, err := s.getObservationBySyncIDTx(ctx, tx, payload.SyncID, true)
	if err == pgx.ErrNoRows {
		_, err = tx.Exec(ctx,
			`INSERT INTO observations (sync_id, session_id, type, title, content, tool_name, project, scope, topic_key, normalized_hash, revision_count, duplicate_count, updated_at, deleted_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, 1, NOW(), NULL)`,
			payload.SyncID, payload.SessionID, payload.Type, payload.Title, payload.Content, payload.ToolName, payload.Project, normalizeScope(payload.Scope), payload.TopicKey, hashNormalized(payload.Content))
		return err
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`UPDATE observations
		 SET session_id = $1, type = $2, title = $3, content = $4, tool_name = $5, project = $6, scope = $7, topic_key = $8, normalized_hash = $9, revision_count = revision_count + 1, updated_at = NOW(), deleted_at = NULL
		 WHERE id = $10`,
		payload.SessionID, payload.Type, payload.Title, payload.Content, payload.ToolName, payload.Project, normalizeScope(payload.Scope), payload.TopicKey, hashNormalized(payload.Content), existing.ID)
	return err
}

func (s *PostgresStore) applyObservationDeleteTx(ctx context.Context, tx pgx.Tx, payload syncObservationPayload) error {
	existing, err := s.getObservationBySyncIDTx(ctx, tx, payload.SyncID, true)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if payload.HardDelete {
		_, err = tx.Exec(ctx, `DELETE FROM observations WHERE id = $1`, existing.ID)
		return err
	}
	deletedAt := payload.DeletedAt
	if deletedAt == nil {
		now := Now()
		deletedAt = &now
	}
	_, err = tx.Exec(ctx,
		`UPDATE observations SET deleted_at = $1, updated_at = NOW() WHERE id = $2`,
		deletedAt, existing.ID)
	return err
}

func (s *PostgresStore) applyPromptUpsertTx(ctx context.Context, tx pgx.Tx, payload syncPromptPayload) error {
	var existingID int64
	err := tx.QueryRow(ctx, `SELECT id FROM user_prompts WHERE sync_id = $1 ORDER BY id DESC LIMIT 1`, payload.SyncID).Scan(&existingID)
	if err == pgx.ErrNoRows {
		_, err = tx.Exec(ctx,
			`INSERT INTO user_prompts (sync_id, session_id, content, project) VALUES ($1, $2, $3, $4)`,
			payload.SyncID, payload.SessionID, payload.Content, payload.Project)
		return err
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`UPDATE user_prompts SET session_id = $1, content = $2, project = $3 WHERE id = $4`,
		payload.SessionID, payload.Content, payload.Project, existingID)
	return err
}

func (s *PostgresStore) backfillProjectSyncMutationsTx(ctx context.Context, tx pgx.Tx, project string) error {
	if err := s.backfillSessionSyncMutationsTx(ctx, tx, project); err != nil {
		return err
	}
	if err := s.backfillObservationSyncMutationsTx(ctx, tx, project); err != nil {
		return err
	}
	return s.backfillPromptSyncMutationsTx(ctx, tx, project)
}

func (s *PostgresStore) repairEnrolledProjectSyncMutations() error {
	ctx := context.Background()
	return s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT project FROM sync_enrolled_projects ORDER BY project ASC`)
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
			if err := s.backfillProjectSyncMutationsTx(ctx, tx, project); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PostgresStore) backfillSessionSyncMutationsTx(ctx context.Context, tx pgx.Tx, project string) error {
	rows, err := tx.Query(ctx, `
		SELECT id, project, directory, ended_at, summary
		FROM sessions
		WHERE project = $1
		  AND NOT EXISTS (
			SELECT 1 FROM sync_mutations sm
			WHERE sm.target_key = $2 AND sm.entity = $3 AND sm.entity_key = sessions.id AND sm.source = $4
		  )
		ORDER BY started_at ASC, id ASC`,
		project, DefaultSyncTargetKey, SyncEntitySession, SyncSourceLocal)
	if err != nil {
		return err
	}
	defer rows.Close()

	var payloads []syncSessionPayload
	for rows.Next() {
		var p syncSessionPayload
		var endedAt *time.Time
		if err := rows.Scan(&p.ID, &p.Project, &p.Directory, &endedAt, &p.Summary); err != nil {
			return err
		}
		p.EndedAt = formatNullableTS(endedAt)
		payloads = append(payloads, p)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, p := range payloads {
		if err := s.enqueueSyncMutationTx(ctx, tx, SyncEntitySession, p.ID, SyncOpUpsert, p); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) backfillObservationSyncMutationsTx(ctx context.Context, tx pgx.Tx, project string) error {
	rows, err := tx.Query(ctx, `
		SELECT sync_id, session_id, type, title, content, tool_name, project, scope, topic_key
		FROM observations
		WHERE COALESCE(project, '') = $1
		  AND deleted_at IS NULL
		  AND NOT EXISTS (
			SELECT 1 FROM sync_mutations sm
			WHERE sm.target_key = $2 AND sm.entity = $3 AND sm.entity_key = observations.sync_id AND sm.source = $4
		  )
		ORDER BY id ASC`,
		project, DefaultSyncTargetKey, SyncEntityObservation, SyncSourceLocal)
	if err != nil {
		return err
	}
	defer rows.Close()

	var payloads []syncObservationPayload
	for rows.Next() {
		var p syncObservationPayload
		if err := rows.Scan(&p.SyncID, &p.SessionID, &p.Type, &p.Title, &p.Content, &p.ToolName, &p.Project, &p.Scope, &p.TopicKey); err != nil {
			return err
		}
		payloads = append(payloads, p)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, p := range payloads {
		if err := s.enqueueSyncMutationTx(ctx, tx, SyncEntityObservation, p.SyncID, SyncOpUpsert, p); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) backfillPromptSyncMutationsTx(ctx context.Context, tx pgx.Tx, project string) error {
	rows, err := tx.Query(ctx, `
		SELECT sync_id, session_id, content, project
		FROM user_prompts
		WHERE COALESCE(project, '') = $1
		  AND NOT EXISTS (
			SELECT 1 FROM sync_mutations sm
			WHERE sm.target_key = $2 AND sm.entity = $3 AND sm.entity_key = user_prompts.sync_id AND sm.source = $4
		  )
		ORDER BY id ASC`,
		project, DefaultSyncTargetKey, SyncEntityPrompt, SyncSourceLocal)
	if err != nil {
		return err
	}
	defer rows.Close()

	var payloads []syncPromptPayload
	for rows.Next() {
		var p syncPromptPayload
		if err := rows.Scan(&p.SyncID, &p.SessionID, &p.Content, &p.Project); err != nil {
			return err
		}
		payloads = append(payloads, p)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, p := range payloads {
		if err := s.enqueueSyncMutationTx(ctx, tx, SyncEntityPrompt, p.SyncID, SyncOpUpsert, p); err != nil {
			return err
		}
	}
	return nil
}

// ─── Shared Helpers (duplicated from store.go for build-tag isolation) ───────
//
// Pure helpers (nullableString, truncate, normalizeScope, derefString,
// hashNormalized, maxInt, newSyncID, normalizeExistingSyncID,
// normalizeSyncTargetKey, stripPrivateTags, sanitizeFTS, normalizeTopicKey,
// normalizeTopicSegment, SuggestTopicKey, inferTopicFamily, hasAny,
// ExtractLearnings, cleanMarkdown, ClassifyTool, NormalizeProject) now live
// in dedicated files without build tags. See common.go, project.go,
// passive_capture.go, sync_payload.go, text_helpers.go, topic_key.go.

func dedupeWindowPG(window time.Duration) string {
	if window <= 0 {
		window = 15 * time.Minute
	}
	minutes := int(window.Minutes())
	if minutes < 1 {
		minutes = 1
	}
	return strconv.Itoa(minutes) + " minutes"
}

// topicKeyAdvisoryLock produces a deterministic int64 from a topic_key+project+scope
// triple, used as a PG advisory lock key to serialize concurrent upserts.
func topicKeyAdvisoryLock(topicKey, project, scope string) int64 {
	h := sha256.Sum256([]byte("engram:topic:" + topicKey + "|" + project + "|" + scope))
	// Use first 8 bytes as int64.
	var v int64
	for i := 0; i < 8; i++ {
		v = (v << 8) | int64(h[i])
	}
	return v
}

// resolveSince converts a human-readable time filter to a UTC timestamp string
// suitable for SQL comparison. Accepts "today", "yesterday", "week", "month",
// or an ISO date like "2026-01-15".
func resolveSince(since string) string {
	now := time.Now().UTC()
	switch strings.TrimSpace(strings.ToLower(since)) {
	case "today":
		y, m, d := now.Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format(tsFormat)
	case "yesterday":
		y, m, d := now.AddDate(0, 0, -1).Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format(tsFormat)
	case "week":
		y, m, d := now.AddDate(0, 0, -7).Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format(tsFormat)
	case "month":
		y, m, d := now.AddDate(0, -1, 0).Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format(tsFormat)
	default:
		// Try ISO date.
		if t, err := time.Parse("2006-01-02", strings.TrimSpace(since)); err == nil {
			return t.UTC().Format(tsFormat)
		}
		// Try full timestamp.
		if t, err := time.Parse(tsFormat, strings.TrimSpace(since)); err == nil {
			return t.UTC().Format(tsFormat)
		}
	}
	return ""
}

// ─── Exported helpers for migrate command ────────────────────────────────────

// ResolveAuthMethodExported exposes resolveAuthMethod for the migration CLI.
// Passes empty dataDir and profile so config file lookup is skipped (migration uses env vars only).
func ResolveAuthMethodExported(connStr string) string {
	return resolveAuthMethod(connStr, "", "")
}

// ConfigurePGPoolExported exposes configurePGPool for the migration CLI.
// ts may be nil (password auth) or any TokenSource implementation.
func ConfigurePGPoolExported(connStr string, ts TokenSource) (*pgxpool.Config, error) {
	return configurePGPool(connStr, ts)
}

// MigratePGExported exposes migratePG for the migration CLI.
func MigratePGExported(pool *pgxpool.Pool) error {
	return migratePG(pool)
}

// NewSyncIDExported exposes newSyncID for the migration CLI.
func NewSyncIDExported(prefix string) string {
	return newSyncID(prefix)
}

// ResolveInteractiveAuthExported exposes resolveInteractiveAuth for the login CLI command.
func ResolveInteractiveAuthExported(dataDir, profile string) (tenantID, clientID string, err error) {
	return resolveInteractiveAuth(dataDir, profile)
}

// ─── Memory Relations — see relations_pg.go ───────────────────────────────────
//
// All PostgresStore relation method implementations (FindCandidates,
// SaveRelation, GetRelation, JudgeRelation, JudgeBySemantic,
// GetRelationsForObservations, ListRelations, CountRelations,
// GetRelationStats, CountDeferredAndDead) live in relations_pg.go.

// ─── Operational diagnostics (doctor subsystem) ──────────────────────────────

// ListDiagnosticSessions returns session evidence scoped by project.
func (s *PostgresStore) ListDiagnosticSessions(project string) ([]DiagnosticSessionEvidence, error) {
	ctx := context.Background()
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	query := `SELECT id, project, COALESCE(directory, ''), id FROM sessions`
	args := []any{}
	if project != "" {
		query += ` WHERE project = $1`
		args = append(args, project)
	}
	query += ` ORDER BY started_at DESC, id ASC`

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessions := make([]DiagnosticSessionEvidence, 0)
	for rows.Next() {
		var ev DiagnosticSessionEvidence
		if err := rows.Scan(&ev.ID, &ev.Project, &ev.Directory, &ev.Name); err != nil {
			return nil, err
		}
		sessions = append(sessions, ev)
	}
	return sessions, rows.Err()
}

// ListPendingProjectMutations returns pending cloud mutations for one project
// or all projects when project is empty.
func (s *PostgresStore) ListPendingProjectMutations(project string) ([]SyncMutation, error) {
	ctx := context.Background()
	project, _ = NormalizeProject(project)
	project = strings.TrimSpace(project)
	argN := 1
	query := fmt.Sprintf(`
		SELECT seq, target_key, entity, entity_key, op, payload, source, project, occurred_at, acked_at
		FROM sync_mutations
		WHERE target_key = $%d AND acked_at IS NULL`, argN)
	argN++
	args := []any{DefaultSyncTargetKey}
	if project != "" {
		query += fmt.Sprintf(` AND project = $%d`, argN)
		args = append(args, project)
	}
	query += ` ORDER BY seq ASC`

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	mutations := make([]SyncMutation, 0)
	for rows.Next() {
		var m SyncMutation
		var occurredAt time.Time
		var ackedAt *time.Time
		if err := rows.Scan(&m.Seq, &m.TargetKey, &m.Entity, &m.EntityKey, &m.Op, &m.Payload, &m.Source, &m.Project, &occurredAt, &ackedAt); err != nil {
			return nil, err
		}
		m.OccurredAt = formatTS(occurredAt)
		m.AckedAt = formatNullableTS(ackedAt)
		mutations = append(mutations, m)
	}
	return mutations, rows.Err()
}

// ReadSQLiteLockSnapshot returns an empty snapshot on PostgreSQL — there are no
// SQLite locks to report. The sqlite_lock_contention check sees no contention.
func (s *PostgresStore) ReadSQLiteLockSnapshot(_ context.Context) (SQLiteLockSnapshot, error) {
	// PostgreSQL does not use SQLite locks. Return a zero-contention snapshot
	// with a positive BusyTimeoutMS so the contention check reports clean.
	return SQLiteLockSnapshot{
		JournalMode:        "pg",
		BusyTimeoutMS:      5000,
		CheckpointBusy:     0,
		CheckpointLog:      0,
		CheckpointedFrames: 0,
	}, nil
}

// EstimateSessionProjectReclassification counts rows that would change without mutating.
func (s *PostgresStore) EstimateSessionProjectReclassification(actions []SessionProjectReclassification) (SessionProjectReclassificationCounts, error) {
	ctx := context.Background()
	var counts SessionProjectReclassificationCounts
	for _, action := range normalizeSessionProjectReclassificationActions(actions) {
		var n int64
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM sessions WHERE id = $1 AND project = $2`, action.SessionID, action.FromProject).Scan(&n); err != nil {
			return counts, fmt.Errorf("estimate sessions: %w", err)
		}
		counts.Sessions += n
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM observations WHERE session_id = $1 AND project = $2 AND deleted_at IS NULL`, action.SessionID, action.FromProject).Scan(&n); err != nil {
			return counts, fmt.Errorf("estimate observations: %w", err)
		}
		counts.Observations += n
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM user_prompts WHERE session_id = $1 AND project = $2`, action.SessionID, action.FromProject).Scan(&n); err != nil {
			return counts, fmt.Errorf("estimate prompts: %w", err)
		}
		counts.Prompts += n
	}
	return counts, nil
}

// ApplySessionProjectReclassification reclassifies sessions, observations, and
// user_prompts atomically. On PostgreSQL there is no SQLite backup; BackupPath is empty.
func (s *PostgresStore) ApplySessionProjectReclassification(actions []SessionProjectReclassification) (SessionProjectReclassificationResult, error) {
	normalized := normalizeSessionProjectReclassificationActions(actions)
	var result SessionProjectReclassificationResult
	// BackupPath is empty on PG — no SQLite file to back up.
	err := s.withTx(context.Background(), func(tx pgx.Tx) error {
		for _, action := range normalized {
			tag, err := tx.Exec(context.Background(),
				`UPDATE sessions SET project = $1 WHERE id = $2 AND project = $3`,
				action.ToProject, action.SessionID, action.FromProject)
			if err != nil {
				return fmt.Errorf("reclassify session %q: %w", action.SessionID, err)
			}
			result.Counts.Sessions += tag.RowsAffected()

			tag, err = tx.Exec(context.Background(),
				`UPDATE observations SET project = $1 WHERE session_id = $2 AND project = $3`,
				action.ToProject, action.SessionID, action.FromProject)
			if err != nil {
				return fmt.Errorf("reclassify observations for session %q: %w", action.SessionID, err)
			}
			result.Counts.Observations += tag.RowsAffected()

			tag, err = tx.Exec(context.Background(),
				`UPDATE user_prompts SET project = $1 WHERE session_id = $2 AND project = $3`,
				action.ToProject, action.SessionID, action.FromProject)
			if err != nil {
				return fmt.Errorf("reclassify prompts for session %q: %w", action.SessionID, err)
			}
			result.Counts.Prompts += tag.RowsAffected()
		}
		return nil
	})
	if err != nil {
		return SessionProjectReclassificationResult{}, err
	}
	return result, nil
}

// BackupSQLite is a no-op on PostgreSQL — there is no SQLite file to back up.
// Returns an empty backup path without error.
func (s *PostgresStore) BackupSQLite() (string, error) {
	return "", nil
}

// CountPendingNonEnrolledSyncMutations is not yet implemented on PostgreSQL.
// PostgreSQL-backed deployments are server-side only; client-side autosync uses SQLite.
func (s *PostgresStore) CountPendingNonEnrolledSyncMutations(_ string) ([]PendingSyncMutationProjectCount, error) {
	return nil, fmt.Errorf("CountPendingNonEnrolledSyncMutations: not implemented for PostgreSQL")
}

// MarkSyncBlocked is not yet implemented on PostgreSQL.
// PostgreSQL-backed deployments are server-side only; client-side autosync uses SQLite.
func (s *PostgresStore) MarkSyncBlocked(_, _, _ string) error {
	return fmt.Errorf("MarkSyncBlocked: not implemented for PostgreSQL")
}

// ReplayDeferred is not yet implemented on PostgreSQL.
// PostgreSQL-backed deployments are server-side only; client-side autosync uses SQLite.
func (s *PostgresStore) ReplayDeferred() (ReplayDeferredResult, error) {
	return ReplayDeferredResult{}, fmt.Errorf("ReplayDeferred: not implemented for PostgreSQL")
}

// Compile-time assertion that *PostgresStore satisfies the Store interface.
var _ Store = (*PostgresStore)(nil)


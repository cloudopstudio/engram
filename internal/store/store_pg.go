//go:build pgstore

// Package store implements the persistent memory engine for Engram.
//
// This file provides the PostgreSQL backend, activated via the `pgstore`
// build tag. It implements the same public API as the SQLite store
// (store.go) so consumers (MCP, HTTP, TUI, CLI) need zero changes.
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Types (duplicated from store.go for build-tag isolation) ────────────────

type Session struct {
	ID        string  `json:"id"`
	Project   string  `json:"project"`
	Directory string  `json:"directory"`
	StartedAt string  `json:"started_at"`
	EndedAt   *string `json:"ended_at,omitempty"`
	Summary   *string `json:"summary,omitempty"`
}

type Observation struct {
	ID             int64   `json:"id"`
	SyncID         string  `json:"sync_id"`
	SessionID      string  `json:"session_id"`
	Type           string  `json:"type"`
	Title          string  `json:"title"`
	Content        string  `json:"content"`
	ToolName       *string `json:"tool_name,omitempty"`
	Project        *string `json:"project,omitempty"`
	Scope          string  `json:"scope"`
	TopicKey       *string `json:"topic_key,omitempty"`
	RevisionCount  int     `json:"revision_count"`
	DuplicateCount int     `json:"duplicate_count"`
	LastSeenAt     *string `json:"last_seen_at,omitempty"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	DeletedAt      *string `json:"deleted_at,omitempty"`
}

type SearchResult struct {
	Observation
	Rank float64 `json:"rank"`
}

type SessionSummary struct {
	ID               string  `json:"id"`
	Project          string  `json:"project"`
	StartedAt        string  `json:"started_at"`
	EndedAt          *string `json:"ended_at,omitempty"`
	Summary          *string `json:"summary,omitempty"`
	ObservationCount int     `json:"observation_count"`
}

type Stats struct {
	TotalSessions     int      `json:"total_sessions"`
	TotalObservations int      `json:"total_observations"`
	TotalPrompts      int      `json:"total_prompts"`
	Projects          []string `json:"projects"`
}

type TimelineEntry struct {
	ID             int64   `json:"id"`
	SessionID      string  `json:"session_id"`
	Type           string  `json:"type"`
	Title          string  `json:"title"`
	Content        string  `json:"content"`
	ToolName       *string `json:"tool_name,omitempty"`
	Project        *string `json:"project,omitempty"`
	Scope          string  `json:"scope"`
	TopicKey       *string `json:"topic_key,omitempty"`
	RevisionCount  int     `json:"revision_count"`
	DuplicateCount int     `json:"duplicate_count"`
	LastSeenAt     *string `json:"last_seen_at,omitempty"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	DeletedAt      *string `json:"deleted_at,omitempty"`
	IsFocus        bool    `json:"is_focus"`
}

type TimelineResult struct {
	Focus        Observation     `json:"focus"`
	Before       []TimelineEntry `json:"before"`
	After        []TimelineEntry `json:"after"`
	SessionInfo  *Session        `json:"session_info"`
	TotalInRange int             `json:"total_in_range"`
}

type SearchOptions struct {
	Type    string `json:"type,omitempty"`
	Project string `json:"project,omitempty"`
	Scope   string `json:"scope,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type AddObservationParams struct {
	SessionID string `json:"session_id"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	Content   string `json:"content"`
	ToolName  string `json:"tool_name,omitempty"`
	Project   string `json:"project,omitempty"`
	Scope     string `json:"scope,omitempty"`
	TopicKey  string `json:"topic_key,omitempty"`
}

type UpdateObservationParams struct {
	Type     *string `json:"type,omitempty"`
	Title    *string `json:"title,omitempty"`
	Content  *string `json:"content,omitempty"`
	Project  *string `json:"project,omitempty"`
	Scope    *string `json:"scope,omitempty"`
	TopicKey *string `json:"topic_key,omitempty"`
}

type Prompt struct {
	ID        int64  `json:"id"`
	SyncID    string `json:"sync_id"`
	SessionID string `json:"session_id"`
	Content   string `json:"content"`
	Project   string `json:"project,omitempty"`
	CreatedAt string `json:"created_at"`
}

type AddPromptParams struct {
	SessionID string `json:"session_id"`
	Content   string `json:"content"`
	Project   string `json:"project,omitempty"`
}

// ─── Constants ───────────────────────────────────────────────────────────────

const (
	DefaultSyncTargetKey = "cloud"

	SyncLifecycleIdle     = "idle"
	SyncLifecyclePending  = "pending"
	SyncLifecycleRunning  = "running"
	SyncLifecycleHealthy  = "healthy"
	SyncLifecycleDegraded = "degraded"

	SyncEntitySession     = "session"
	SyncEntityObservation = "observation"
	SyncEntityPrompt      = "prompt"

	SyncOpUpsert = "upsert"
	SyncOpDelete = "delete"

	SyncSourceLocal  = "local"
	SyncSourceRemote = "remote"
)

type SyncState struct {
	TargetKey           string  `json:"target_key"`
	Lifecycle           string  `json:"lifecycle"`
	LastEnqueuedSeq     int64   `json:"last_enqueued_seq"`
	LastAckedSeq        int64   `json:"last_acked_seq"`
	LastPulledSeq       int64   `json:"last_pulled_seq"`
	ConsecutiveFailures int     `json:"consecutive_failures"`
	BackoffUntil        *string `json:"backoff_until,omitempty"`
	LeaseOwner          *string `json:"lease_owner,omitempty"`
	LeaseUntil          *string `json:"lease_until,omitempty"`
	LastError           *string `json:"last_error,omitempty"`
	UpdatedAt           string  `json:"updated_at"`
}

type SyncMutation struct {
	Seq        int64   `json:"seq"`
	TargetKey  string  `json:"target_key"`
	Entity     string  `json:"entity"`
	EntityKey  string  `json:"entity_key"`
	Op         string  `json:"op"`
	Payload    string  `json:"payload"`
	Source     string  `json:"source"`
	Project    string  `json:"project"`
	OccurredAt string  `json:"occurred_at"`
	AckedAt    *string `json:"acked_at,omitempty"`
}

type EnrolledProject struct {
	Project    string `json:"project"`
	EnrolledAt string `json:"enrolled_at"`
}

type syncSessionPayload struct {
	ID        string  `json:"id"`
	Project   string  `json:"project"`
	Directory string  `json:"directory"`
	EndedAt   *string `json:"ended_at,omitempty"`
	Summary   *string `json:"summary,omitempty"`
}

type syncObservationPayload struct {
	SyncID     string  `json:"sync_id"`
	SessionID  string  `json:"session_id"`
	Type       string  `json:"type"`
	Title      string  `json:"title"`
	Content    string  `json:"content"`
	ToolName   *string `json:"tool_name,omitempty"`
	Project    *string `json:"project,omitempty"`
	Scope      string  `json:"scope"`
	TopicKey   *string `json:"topic_key,omitempty"`
	Deleted    bool    `json:"deleted,omitempty"`
	DeletedAt  *string `json:"deleted_at,omitempty"`
	HardDelete bool    `json:"hard_delete,omitempty"`
}

type syncPromptPayload struct {
	SyncID    string  `json:"sync_id"`
	SessionID string  `json:"session_id"`
	Content   string  `json:"content"`
	Project   *string `json:"project,omitempty"`
}

type ExportData struct {
	Version      string        `json:"version"`
	ExportedAt   string        `json:"exported_at"`
	Sessions     []Session     `json:"sessions"`
	Observations []Observation `json:"observations"`
	Prompts      []Prompt      `json:"prompts"`
}

type ImportResult struct {
	SessionsImported     int `json:"sessions_imported"`
	ObservationsImported int `json:"observations_imported"`
	PromptsImported      int `json:"prompts_imported"`
}

type MigrateResult struct {
	Migrated            bool  `json:"migrated"`
	ObservationsUpdated int64 `json:"observations_updated"`
	SessionsUpdated     int64 `json:"sessions_updated"`
	PromptsUpdated      int64 `json:"prompts_updated"`
}

type PassiveCaptureParams struct {
	SessionID string `json:"session_id"`
	Content   string `json:"content"`
	Project   string `json:"project,omitempty"`
	Source    string `json:"source,omitempty"`
}

type PassiveCaptureResult struct {
	Extracted  int `json:"extracted"`
	Saved      int `json:"saved"`
	Duplicates int `json:"duplicates"`
}

// ─── Config ──────────────────────────────────────────────────────────────────

type Config struct {
	DataDir              string
	MaxObservationLength int
	MaxContextResults    int
	MaxSearchResults     int
	DedupeWindow         time.Duration
}

func DefaultConfig() (Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Config{}, fmt.Errorf("engram: determine home directory: %w", err)
	}
	return Config{
		DataDir:              filepath.Join(home, ".engram"),
		MaxObservationLength: 50000,
		MaxContextResults:    20,
		MaxSearchResults:     20,
		DedupeWindow:         15 * time.Minute,
	}, nil
}

func FallbackConfig(dataDir string) Config {
	return Config{
		DataDir:              dataDir,
		MaxObservationLength: 50000,
		MaxContextResults:    20,
		MaxSearchResults:     20,
		DedupeWindow:         15 * time.Minute,
	}
}

// ─── Store ───────────────────────────────────────────────────────────────────

// Store is the PostgreSQL-backed persistent memory engine.
type Store struct {
	pool     *pgxpool.Pool
	cfg      Config
	identity string // Entra ID email, populated if auth method is entra.
}

// New creates a PG-backed Store by reading ENGRAM_DATABASE_URL.
func New(cfg Config) (*Store, error) {
	connStr := os.Getenv("ENGRAM_DATABASE_URL")
	if connStr == "" {
		return nil, fmt.Errorf("engram: ENGRAM_DATABASE_URL must be set for PostgreSQL mode")
	}

	authMethod := resolveAuthMethod(connStr)

	var tp *TokenProvider
	var identity string
	if authMethod != "password" {
		var err error
		tp, err = NewTokenProvider()
		if err != nil {
			return nil, fmt.Errorf("engram: entra auth failed (set ENGRAM_AUTH_METHOD=password to use password): %w", err)
		}
		// Acquire an initial token to populate identity.
		if _, err := tp.Token(context.Background()); err != nil {
			log.Printf("[engram] warning: initial token acquisition failed: %v", err)
		} else {
			identity = tp.Identity()
		}
	}

	pgxCfg, err := configurePGPool(connStr, tp)
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

	s := &Store{pool: pool, cfg: cfg, identity: identity}
	if err := s.repairEnrolledProjectSyncMutations(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("engram: repair enrolled sync journal: %w", err)
	}

	return s, nil
}

func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

func (s *Store) MaxObservationLength() int {
	return s.cfg.MaxObservationLength
}

// ─── Transaction helper ──────────────────────────────────────────────────────

func (s *Store) withTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
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

const tsFormat = "2006-01-02 15:04:05"

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

// Now returns the current time formatted for compatibility with the SQLite store.
func Now() string {
	return time.Now().UTC().Format(tsFormat)
}

// ─── Sessions ────────────────────────────────────────────────────────────────

func (s *Store) CreateSession(id, project, directory string) error {
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

func (s *Store) createSessionTx(ctx context.Context, tx pgx.Tx, id, project, directory string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO sessions (id, project, directory, created_by) VALUES ($1, $2, $3, $4)
		 ON CONFLICT(id) DO UPDATE SET
		   project   = CASE WHEN sessions.project = '' THEN EXCLUDED.project ELSE sessions.project END,
		   directory = CASE WHEN sessions.directory = '' THEN EXCLUDED.directory ELSE sessions.directory END`,
		id, project, directory, s.identity,
	)
	return err
}

func (s *Store) EndSession(id string, summary string) error {
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

func (s *Store) GetSession(id string) (*Session, error) {
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

func (s *Store) RecentSessions(project string, limit int) ([]SessionSummary, error) {
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

func (s *Store) AllSessions(project string, limit int) ([]SessionSummary, error) {
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

func (s *Store) AddObservation(p AddObservationParams) (int64, error) {
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

func (s *Store) GetObservation(id int64) (*Observation, error) {
	ctx := context.Background()
	return s.scanObservation(ctx, s.pool,
		`SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE id = $1 AND deleted_at IS NULL`, id)
}

func (s *Store) UpdateObservation(id int64, p UpdateObservationParams) (*Observation, error) {
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

func (s *Store) DeleteObservation(id int64, hardDelete bool) error {
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

func (s *Store) AllObservations(project, scope string, limit int) ([]Observation, error) {
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

func (s *Store) SessionObservations(sessionID string, limit int) ([]Observation, error) {
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

func (s *Store) RecentObservations(project, scope string, limit int) ([]Observation, error) {
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

func (s *Store) GetObservationBySyncID(syncID string) (*Observation, error) {
	ctx := context.Background()
	return s.scanObservation(ctx, s.pool,
		`SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE sync_id = $1 AND deleted_at IS NULL ORDER BY id DESC LIMIT 1`, syncID)
}

// ─── User Prompts ────────────────────────────────────────────────────────────

func (s *Store) AddPrompt(p AddPromptParams) (int64, error) {
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

func (s *Store) RecentPrompts(project string, limit int) ([]Prompt, error) {
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

func (s *Store) SearchPrompts(query string, project string, limit int) ([]Prompt, error) {
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

func (s *Store) Search(query string, opts SearchOptions) ([]SearchResult, error) {
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

func (s *Store) Timeline(observationID int64, before, after int) (*TimelineResult, error) {
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

func (s *Store) Stats() (*Stats, error) {
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

func (s *Store) FormatContext(project, scope string) (string, error) {
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

func (s *Store) Export() (*ExportData, error) {
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

func (s *Store) Import(data *ExportData) (*ImportResult, error) {
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

func (s *Store) GetSyncedChunks() (map[string]bool, error) {
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

func (s *Store) RecordSyncedChunk(chunkID string) error {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx,
		"INSERT INTO sync_chunks (chunk_id) VALUES ($1) ON CONFLICT(chunk_id) DO NOTHING", chunkID)
	return err
}

// ─── Sync State ──────────────────────────────────────────────────────────────

func (s *Store) GetSyncState(targetKey string) (*SyncState, error) {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	if err := s.ensureSyncState(ctx, targetKey); err != nil {
		return nil, err
	}
	return s.getSyncState(ctx, targetKey)
}

func (s *Store) ListPendingSyncMutations(targetKey string, limit int) ([]SyncMutation, error) {
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

func (s *Store) SkipAckNonEnrolledMutations(targetKey string) (int64, error) {
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

func (s *Store) AckSyncMutations(targetKey string, lastAckedSeq int64) error {
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

func (s *Store) AckSyncMutationSeqs(targetKey string, seqs []int64) error {
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

func (s *Store) AcquireSyncLease(targetKey, owner string, ttl time.Duration, now time.Time) (bool, error) {
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

func (s *Store) ReleaseSyncLease(targetKey, owner string) error {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	_, err := s.pool.Exec(ctx,
		`UPDATE sync_state SET lease_owner = NULL, lease_until = NULL, updated_at = NOW()
		 WHERE target_key = $1 AND (lease_owner = $2 OR lease_owner IS NULL OR lease_owner = '')`,
		targetKey, owner)
	return err
}

func (s *Store) MarkSyncFailure(targetKey, message string, backoffUntil time.Time) error {
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

func (s *Store) MarkSyncHealthy(targetKey string) error {
	ctx := context.Background()
	targetKey = normalizeSyncTargetKey(targetKey)
	_, err := s.pool.Exec(ctx,
		`UPDATE sync_state
		 SET lifecycle = $1, consecutive_failures = 0, backoff_until = NULL, last_error = NULL, updated_at = NOW()
		 WHERE target_key = $2`,
		SyncLifecycleHealthy, targetKey)
	return err
}

func (s *Store) ApplyPulledMutation(targetKey string, mutation SyncMutation) error {
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

func (s *Store) EnrollProject(project string) error {
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

func (s *Store) UnenrollProject(project string) error {
	if project == "" {
		return fmt.Errorf("project name must not be empty")
	}
	ctx := context.Background()
	_, err := s.pool.Exec(ctx,
		`DELETE FROM sync_enrolled_projects WHERE project = $1`, project)
	return err
}

func (s *Store) ListEnrolledProjects() ([]EnrolledProject, error) {
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

func (s *Store) IsProjectEnrolled(project string) (bool, error) {
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

func (s *Store) MigrateProject(oldName, newName string) (*MigrateResult, error) {
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

// ─── Passive Capture ─────────────────────────────────────────────────────────

func (s *Store) PassiveCapture(p PassiveCaptureParams) (*PassiveCaptureResult, error) {
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

func (s *Store) scanObservation(ctx context.Context, q pgQuerier, sql string, args ...any) (*Observation, error) {
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

func (s *Store) getObservationTx(ctx context.Context, tx pgx.Tx, id int64) (*Observation, error) {
	return s.scanObservation(ctx, tx,
		`SELECT id, COALESCE(sync_id, '') as sync_id, session_id, type, title, content, tool_name, project,
		        scope, topic_key, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at
		 FROM observations WHERE id = $1 AND deleted_at IS NULL`, id)
}

func (s *Store) getObservationBySyncIDTx(ctx context.Context, tx pgx.Tx, syncID string, includeDeleted bool) (*Observation, error) {
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

func (s *Store) queryObservationsPG(ctx context.Context, query string, args ...any) ([]Observation, error) {
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

func observationPayloadFromObservation(obs *Observation) syncObservationPayload {
	return syncObservationPayload{
		SyncID:    obs.SyncID,
		SessionID: obs.SessionID,
		Type:      obs.Type,
		Title:     obs.Title,
		Content:   obs.Content,
		ToolName:  obs.ToolName,
		Project:   obs.Project,
		Scope:     obs.Scope,
		TopicKey:  obs.TopicKey,
	}
}

func (s *Store) ensureSyncState(ctx context.Context, targetKey string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO sync_state (target_key, lifecycle, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT(target_key) DO NOTHING`,
		targetKey, SyncLifecycleIdle)
	return err
}

func (s *Store) getSyncState(ctx context.Context, targetKey string) (*SyncState, error) {
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

func (s *Store) getSyncStateTx(ctx context.Context, tx pgx.Tx, targetKey string) (*SyncState, error) {
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

func (s *Store) enqueueSyncMutationTx(ctx context.Context, tx pgx.Tx, entity, entityKey, op string, payload any) error {
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

func extractProjectFromPayload(payload any) string {
	switch p := payload.(type) {
	case syncSessionPayload:
		return p.Project
	case syncObservationPayload:
		if p.Project != nil {
			return *p.Project
		}
		return ""
	case syncPromptPayload:
		if p.Project != nil {
			return *p.Project
		}
		return ""
	default:
		data, err := json.Marshal(payload)
		if err != nil {
			return ""
		}
		var generic struct {
			Project *string `json:"project"`
		}
		if err := json.Unmarshal(data, &generic); err != nil || generic.Project == nil {
			return ""
		}
		return *generic.Project
	}
}

func decodeSyncPayload(payload []byte, dest any) error {
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" {
		return fmt.Errorf("empty payload")
	}
	if trimmed[0] != '"' {
		return json.Unmarshal([]byte(trimmed), dest)
	}
	var encoded string
	if err := json.Unmarshal([]byte(trimmed), &encoded); err != nil {
		return err
	}
	return json.Unmarshal([]byte(encoded), dest)
}

func (s *Store) applySessionPayloadTx(ctx context.Context, tx pgx.Tx, payload syncSessionPayload) error {
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

func (s *Store) applyObservationUpsertTx(ctx context.Context, tx pgx.Tx, payload syncObservationPayload) error {
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

func (s *Store) applyObservationDeleteTx(ctx context.Context, tx pgx.Tx, payload syncObservationPayload) error {
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

func (s *Store) applyPromptUpsertTx(ctx context.Context, tx pgx.Tx, payload syncPromptPayload) error {
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

func (s *Store) backfillProjectSyncMutationsTx(ctx context.Context, tx pgx.Tx, project string) error {
	if err := s.backfillSessionSyncMutationsTx(ctx, tx, project); err != nil {
		return err
	}
	if err := s.backfillObservationSyncMutationsTx(ctx, tx, project); err != nil {
		return err
	}
	return s.backfillPromptSyncMutationsTx(ctx, tx, project)
}

func (s *Store) repairEnrolledProjectSyncMutations() error {
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

func (s *Store) backfillSessionSyncMutationsTx(ctx context.Context, tx pgx.Tx, project string) error {
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

func (s *Store) backfillObservationSyncMutationsTx(ctx context.Context, tx pgx.Tx, project string) error {
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

func (s *Store) backfillPromptSyncMutationsTx(ctx context.Context, tx pgx.Tx, project string) error {
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

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func truncate(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max]) + "..."
}

func normalizeScope(scope string) string {
	v := strings.TrimSpace(strings.ToLower(scope))
	if v == "personal" {
		return "personal"
	}
	return "project"
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func hashNormalized(content string) string {
	normalized := strings.ToLower(strings.Join(strings.Fields(content), " "))
	h := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(h[:])
}

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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
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

func normalizeSyncTargetKey(targetKey string) string {
	if strings.TrimSpace(targetKey) == "" {
		return DefaultSyncTargetKey
	}
	return strings.TrimSpace(strings.ToLower(targetKey))
}

func newSyncID(prefix string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(b)
}

func normalizeExistingSyncID(existing, prefix string) string {
	if strings.TrimSpace(existing) != "" {
		return existing
	}
	return newSyncID(prefix)
}

var privateTagRegex = regexp.MustCompile(`(?is)<private>.*?</private>`)

func stripPrivateTags(s string) string {
	result := privateTagRegex.ReplaceAllString(s, "[REDACTED]")
	return strings.TrimSpace(result)
}

func sanitizeFTS(query string) string {
	words := strings.Fields(query)
	for i, w := range words {
		w = strings.Trim(w, `"`)
		words[i] = `"` + w + `"`
	}
	return strings.Join(words, " ")
}

func normalizeTopicKey(topic string) string {
	v := strings.TrimSpace(strings.ToLower(topic))
	if v == "" {
		return ""
	}
	v = strings.Join(strings.Fields(v), "-")
	if len(v) > 120 {
		v = v[:120]
	}
	return v
}

func normalizeTopicSegment(s string) string {
	v := strings.ToLower(strings.TrimSpace(s))
	if v == "" {
		return ""
	}
	re := regexp.MustCompile(`[^a-z0-9]+`)
	v = re.ReplaceAllString(v, " ")
	v = strings.Join(strings.Fields(v), "-")
	if len(v) > 100 {
		v = v[:100]
	}
	return v
}

// SuggestTopicKey generates a stable topic key suggestion from type/title/content.
func SuggestTopicKey(typ, title, content string) string {
	family := inferTopicFamily(typ, title, content)
	cleanTitle := stripPrivateTags(title)
	segment := normalizeTopicSegment(cleanTitle)

	if segment == "" {
		cleanContent := stripPrivateTags(content)
		words := strings.Fields(strings.ToLower(cleanContent))
		if len(words) > 8 {
			words = words[:8]
		}
		segment = normalizeTopicSegment(strings.Join(words, " "))
	}

	if segment == "" {
		segment = "general"
	}

	if strings.HasPrefix(segment, family+"-") {
		segment = strings.TrimPrefix(segment, family+"-")
	}
	if segment == "" || segment == family {
		segment = "general"
	}

	return family + "/" + segment
}

func inferTopicFamily(typ, title, content string) string {
	t := strings.TrimSpace(strings.ToLower(typ))
	switch t {
	case "architecture", "design", "adr", "refactor":
		return "architecture"
	case "bug", "bugfix", "fix", "incident", "hotfix":
		return "bug"
	case "decision":
		return "decision"
	case "pattern", "convention", "guideline":
		return "pattern"
	case "config", "setup", "infra", "infrastructure", "ci":
		return "config"
	case "discovery", "investigation", "root_cause", "root-cause":
		return "discovery"
	case "learning", "learn":
		return "learning"
	case "session_summary":
		return "session"
	}

	text := strings.ToLower(title + " " + content)
	if hasAny(text, "bug", "fix", "panic", "error", "crash", "regression", "incident", "hotfix") {
		return "bug"
	}
	if hasAny(text, "architecture", "design", "adr", "boundary", "hexagonal", "refactor") {
		return "architecture"
	}
	if hasAny(text, "decision", "tradeoff", "chose", "choose", "decide") {
		return "decision"
	}
	if hasAny(text, "pattern", "convention", "naming", "guideline") {
		return "pattern"
	}
	if hasAny(text, "config", "setup", "environment", "env", "docker", "pipeline") {
		return "config"
	}
	if hasAny(text, "discovery", "investigate", "investigation", "found", "root cause") {
		return "discovery"
	}
	if hasAny(text, "learned", "learning") {
		return "learning"
	}

	if t != "" && t != "manual" {
		return normalizeTopicSegment(t)
	}

	return "topic"
}

func hasAny(text string, words ...string) bool {
	for _, w := range words {
		if strings.Contains(text, w) {
			return true
		}
	}
	return false
}

// ExtractLearnings parses structured learning items from text.
var learningHeaderPattern = regexp.MustCompile(
	`(?im)^#{2,3}\s+(?:Aprendizajes(?:\s+Clave)?|Key\s+Learnings?|Learnings?):?\s*$`,
)

const (
	minLearningLength = 20
	minLearningWords  = 4
)

func ExtractLearnings(text string) []string {
	matches := learningHeaderPattern.FindAllStringIndex(text, -1)
	if len(matches) == 0 {
		return nil
	}

	for i := len(matches) - 1; i >= 0; i-- {
		sectionStart := matches[i][1]
		sectionText := text[sectionStart:]

		if nextHeader := regexp.MustCompile(`\n#{1,3} `).FindStringIndex(sectionText); nextHeader != nil {
			sectionText = sectionText[:nextHeader[0]]
		}

		var learnings []string

		numbered := regexp.MustCompile(`(?m)^\s*\d+[.)]\s+(.+)`).FindAllStringSubmatch(sectionText, -1)
		if len(numbered) > 0 {
			for _, m := range numbered {
				cleaned := cleanMarkdown(m[1])
				if len(cleaned) >= minLearningLength && len(strings.Fields(cleaned)) >= minLearningWords {
					learnings = append(learnings, cleaned)
				}
			}
		}

		if len(learnings) == 0 {
			bullets := regexp.MustCompile(`(?m)^\s*[-*]\s+(.+)`).FindAllStringSubmatch(sectionText, -1)
			for _, m := range bullets {
				cleaned := cleanMarkdown(m[1])
				if len(cleaned) >= minLearningLength && len(strings.Fields(cleaned)) >= minLearningWords {
					learnings = append(learnings, cleaned)
				}
			}
		}

		if len(learnings) > 0 {
			return learnings
		}
	}

	return nil
}

func cleanMarkdown(text string) string {
	text = regexp.MustCompile(`\*\*([^*]+)\*\*`).ReplaceAllString(text, "$1")
	text = regexp.MustCompile("`([^`]+)`").ReplaceAllString(text, "$1")
	text = regexp.MustCompile(`\*([^*]+)\*`).ReplaceAllString(text, "$1")
	return strings.TrimSpace(strings.Join(strings.Fields(text), " "))
}

// ClassifyTool returns the observation type for a given tool name.
func ClassifyTool(toolName string) string {
	switch toolName {
	case "write", "edit", "patch":
		return "file_change"
	case "bash":
		return "command"
	case "read", "view":
		return "file_read"
	case "grep", "glob", "ls":
		return "search"
	default:
		return "tool_use"
	}
}

// ─── Exported helpers for migrate command ────────────────────────────────────

// ResolveAuthMethodExported exposes resolveAuthMethod for the migration CLI.
func ResolveAuthMethodExported(connStr string) string {
	return resolveAuthMethod(connStr)
}

// ConfigurePGPoolExported exposes configurePGPool for the migration CLI.
func ConfigurePGPoolExported(connStr string, tp *TokenProvider) (*pgxpool.Config, error) {
	return configurePGPool(connStr, tp)
}

// MigratePGExported exposes migratePG for the migration CLI.
func MigratePGExported(pool *pgxpool.Pool) error {
	return migratePG(pool)
}

// NewSyncIDExported exposes newSyncID for the migration CLI.
func NewSyncIDExported(prefix string) string {
	return newSyncID(prefix)
}

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// PostgreSQL implementations of the Store methods introduced by the upstream
// v1.20.0 sync. They mirror the SQLite behaviour in store.go; the SQL is
// translated to Postgres (numbered placeholders, NOW(), COALESCE, ON CONFLICT).
//
// A few deep cloud-sync methods are explicit not-implemented stubs, matching the
// existing precedent for MarkSyncBlocked: the PostgreSQL backend is used for
// team/server deployments that do not drive the local chunk-sync rails.

// ─── Observation pinning ─────────────────────────────────────────────────────

func (s *PostgresStore) setObservationPinned(id int64, pinned bool) error {
	ctx := context.Background()
	tag, err := s.pool.Exec(ctx,
		`UPDATE observations SET pinned = $1, updated_at = NOW() WHERE id = $2 AND deleted_at IS NULL`,
		pinned, id)
	if err != nil {
		return fmt.Errorf("set observation pinned: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrObservationNotFound
	}
	return nil
}

// PinObservation marks an observation as pinned for context priority.
func (s *PostgresStore) PinObservation(id int64) error { return s.setObservationPinned(id, true) }

// UnpinObservation clears the pinned flag on an observation.
func (s *PostgresStore) UnpinObservation(id int64) error { return s.setObservationPinned(id, false) }

// ─── Review lifecycle ────────────────────────────────────────────────────────

// MarkReviewed pushes an observation's review_after forward by the decay window
// configured for its type, or clears it when the type has no decay policy.
func (s *PostgresStore) MarkReviewed(id int64) error {
	ctx := context.Background()
	var obsType string
	err := s.pool.QueryRow(ctx,
		`SELECT type FROM observations WHERE id = $1 AND deleted_at IS NULL`, id).Scan(&obsType)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrObservationNotFound
	}
	if err != nil {
		return fmt.Errorf("mark reviewed: load observation: %w", err)
	}

	var reviewAfter any
	if months, ok := decayReviewAfterMonths[obsType]; ok {
		reviewAfter = time.Now().UTC().AddDate(0, months, 0)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE observations SET review_after = $1, updated_at = NOW() WHERE id = $2 AND deleted_at IS NULL`,
		reviewAfter, id); err != nil {
		return fmt.Errorf("mark reviewed: %w", err)
	}
	return nil
}

// ObservationsNeedingReview returns observations whose review_after has elapsed,
// oldest first.
func (s *PostgresStore) ObservationsNeedingReview(project string, limit int) ([]Observation, error) {
	project, _ = NormalizeProject(project)
	if limit <= 0 {
		limit = s.cfg.MaxContextResults
	}

	ctx := context.Background()
	query := `
		SELECT o.id, COALESCE(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
		       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.created_at, o.updated_at, o.deleted_at
		FROM observations o
		WHERE o.deleted_at IS NULL
		  AND o.review_after IS NOT NULL
		  AND o.review_after <= NOW()`
	args := []any{}
	if project != "" {
		args = append(args, project)
		query += fmt.Sprintf(" AND LOWER(o.project) = $%d", len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY o.review_after ASC, o.id ASC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("observations needing review: %w", err)
	}
	defer rows.Close()

	var out []Observation
	for rows.Next() {
		obs, err := scanObservationRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, obs)
	}
	return out, rows.Err()
}

// ─── Session resolution ──────────────────────────────────────────────────────

// MostRecentActiveSession resolves the newest un-ended session for a project so
// separate processes converge on the same implicit session.
func (s *PostgresStore) MostRecentActiveSession(project string) (string, bool, error) {
	project, _ = NormalizeProject(project)
	if project == "" {
		return "", false, nil
	}

	ctx := context.Background()
	var id string
	err := s.pool.QueryRow(ctx, `
		SELECT id
		FROM sessions
		WHERE LOWER(project) = $1
		  AND ended_at IS NULL
		  AND id NOT LIKE 'manual-save%'
		ORDER BY started_at DESC, id DESC
		LIMIT 1
	`, project).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("most recent active session: %w", err)
	}
	return id, true, nil
}

// ─── Prompts ─────────────────────────────────────────────────────────────────

// AddPromptIfMissing inserts a prompt only when an identical one is not already
// recorded for the session. The bool reports whether a row was inserted.
func (s *PostgresStore) AddPromptIfMissing(p AddPromptParams) (int64, bool, error) {
	p.Project, _ = NormalizeProject(p.Project)
	content := stripPrivateTags(p.Content)
	if len(content) > s.cfg.MaxObservationLength {
		content = content[:s.cfg.MaxObservationLength] + "... [truncated]"
	}

	ctx := context.Background()
	var promptID int64
	inserted := false
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx,
			`SELECT id FROM user_prompts
			 WHERE session_id = $1 AND COALESCE(project, '') = $2 AND content = $3
			 ORDER BY id DESC LIMIT 1`,
			p.SessionID, p.Project, content,
		).Scan(&promptID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		syncID := newSyncID("prompt")
		if err := tx.QueryRow(ctx,
			`INSERT INTO user_prompts (sync_id, session_id, content, project, created_by)
			 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			syncID, p.SessionID, content, nullableString(p.Project), s.identity,
		).Scan(&promptID); err != nil {
			return err
		}
		inserted = true
		return s.enqueueSyncMutationTx(ctx, tx, SyncEntityPrompt, syncID, SyncOpUpsert, syncPromptPayload{
			SyncID:    syncID,
			SessionID: p.SessionID,
			Content:   content,
			Project:   nullableString(p.Project),
		})
	})
	if err != nil {
		return 0, false, err
	}
	return promptID, inserted, nil
}

// ─── Per-target sync chunks ──────────────────────────────────────────────────

// GetSyncedChunksForTarget returns the chunk IDs already applied for a target.
func (s *PostgresStore) GetSyncedChunksForTarget(targetKey string) (map[string]bool, error) {
	targetKey = normalizeChunkTargetKey(targetKey)
	ctx := context.Background()
	rows, err := s.pool.Query(ctx,
		`SELECT chunk_id FROM sync_chunks WHERE target_key = $1`, targetKey)
	if err != nil {
		return nil, fmt.Errorf("get synced chunks for target: %w", err)
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

// RecordSyncedChunkForTarget marks a chunk as applied for a target.
func (s *PostgresStore) RecordSyncedChunkForTarget(targetKey, chunkID string) error {
	targetKey = normalizeChunkTargetKey(targetKey)
	ctx := context.Background()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO sync_chunks (target_key, chunk_id) VALUES ($1, $2)
		 ON CONFLICT (target_key, chunk_id) DO NOTHING`, targetKey, chunkID)
	if err != nil {
		return fmt.Errorf("record synced chunk for target: %w", err)
	}
	return nil
}

// ─── Project-scoped export ───────────────────────────────────────────────────

// ExportProject returns an export restricted to a single normalized project so
// callers avoid dumping the whole database to sync one project.
func (s *PostgresStore) ExportProject(project string) (*ExportData, error) {
	project, _ = NormalizeProject(project)
	if project == "" {
		return s.Export()
	}

	full, err := s.Export()
	if err != nil {
		return nil, err
	}

	out := &ExportData{Version: full.Version, ExportedAt: full.ExportedAt}
	sessionIDs := make(map[string]bool)
	for _, sess := range full.Sessions {
		if normalized, _ := NormalizeProject(sess.Project); normalized == project {
			out.Sessions = append(out.Sessions, sess)
			sessionIDs[sess.ID] = true
		}
	}
	for _, obs := range full.Observations {
		obsProject := derefString(obs.Project)
		normalized, _ := NormalizeProject(obsProject)
		if normalized == project || (obsProject == "" && sessionIDs[obs.SessionID]) {
			out.Observations = append(out.Observations, obs)
		}
	}
	for _, prompt := range full.Prompts {
		normalized, _ := NormalizeProject(prompt.Project)
		if normalized == project || (prompt.Project == "" && sessionIDs[prompt.SessionID]) {
			out.Prompts = append(out.Prompts, prompt)
		}
	}
	return out, nil
}

// ─── Not implemented on PostgreSQL ───────────────────────────────────────────
//
// These drive the local file/chunk sync rails and the cloud-upgrade state
// machine, both of which are SQLite-only paths today. Returning an explicit
// error matches the existing MarkSyncBlocked precedent and keeps the failure
// loud rather than silently doing nothing.

func (s *PostgresStore) ApplyPulledChunk(_, _ string, _ []SyncMutation) error {
	return fmt.Errorf("ApplyPulledChunk: not implemented for PostgreSQL")
}

func (s *PostgresStore) ListPendingSyncMutationsAfterSeq(_ string, _ int64, _ int) ([]SyncMutation, error) {
	return nil, fmt.Errorf("ListPendingSyncMutationsAfterSeq: not implemented for PostgreSQL")
}

func (s *PostgresStore) ExportRelationMutations(_ string) ([]SyncMutation, error) {
	return nil, fmt.Errorf("ExportRelationMutations: not implemented for PostgreSQL")
}

func (s *PostgresStore) GetCloudUpgradeState(_ string) (*CloudUpgradeState, error) {
	return nil, fmt.Errorf("GetCloudUpgradeState: not implemented for PostgreSQL")
}

func (s *PostgresStore) CanRollbackCloudUpgrade(_ string) (bool, error) {
	return false, fmt.Errorf("CanRollbackCloudUpgrade: not implemented for PostgreSQL")
}

func (s *PostgresStore) RollbackCloudUpgrade(_ string) (CloudUpgradeState, error) {
	return CloudUpgradeState{}, fmt.Errorf("RollbackCloudUpgrade: not implemented for PostgreSQL")
}

func (s *PostgresStore) SaveCloudUpgradeState(_ CloudUpgradeState) error {
	return fmt.Errorf("SaveCloudUpgradeState: not implemented for PostgreSQL")
}

// GetRelationByIntID retrieves a relation enriched with source/target titles by
// its integer primary key.
func (s *PostgresStore) GetRelationByIntID(id int64) (*RelationListItem, error) {
	ctx := context.Background()
	var it RelationListItem
	err := s.pool.QueryRow(ctx, `
		SELECT r.id, r.sync_id, r.relation, r.judgment_status,
		       COALESCE(r.source_id,''), COALESCE(src.title,''),
		       COALESCE(r.target_id,''), COALESCE(tgt.title,''),
		       r.created_at, r.updated_at
		FROM memory_relations r
		LEFT JOIN observations src ON src.sync_id = r.source_id
		LEFT JOIN observations tgt ON tgt.sync_id = r.target_id
		WHERE r.id = $1
	`, id).Scan(&it.ID, &it.SyncID, &it.Relation, &it.JudgmentStatus,
		&it.SourceID, &it.SourceTitle, &it.TargetID, &it.TargetTitle,
		&it.CreatedAt, &it.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("relation #%d not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("get relation by int id: %w", err)
	}
	return &it, nil
}

func (s *PostgresStore) DiagnoseCloudUpgradeLegacyMutations(_ string) (CloudUpgradeLegacyMutationReport, error) {
	return CloudUpgradeLegacyMutationReport{}, fmt.Errorf("DiagnoseCloudUpgradeLegacyMutations: not implemented for PostgreSQL")
}

func (s *PostgresStore) RepairCloudUpgrade(_ string, _ bool) (CloudUpgradeRepairReport, error) {
	return CloudUpgradeRepairReport{}, fmt.Errorf("RepairCloudUpgrade: not implemented for PostgreSQL")
}

func (s *PostgresStore) DeleteProject(_ string, _ bool) (*DeleteProjectResult, error) {
	return nil, fmt.Errorf("DeleteProject: not implemented for PostgreSQL")
}

func (s *PostgresStore) MarkSyncAuthRequired(_, _ string) error {
	return fmt.Errorf("MarkSyncAuthRequired: not implemented for PostgreSQL")
}

func (s *PostgresStore) HasPendingSyncMutationsForProject(_ string) (bool, error) {
	return false, fmt.Errorf("HasPendingSyncMutationsForProject: not implemented for PostgreSQL")
}

func (s *PostgresStore) MarkSyncPending(_ string) error {
	return fmt.Errorf("MarkSyncPending: not implemented for PostgreSQL")
}

// DB satisfies the Store test seam. The PostgreSQL backend is pgx-based and has
// no *sql.DB, so callers must treat a nil result as "not available".
func (s *PostgresStore) DB() *sql.DB { return nil }

func (s *PostgresStore) ClearCloudUpgradeState(_ string) error {
	return fmt.Errorf("ClearCloudUpgradeState: not implemented for PostgreSQL")
}

func (s *PostgresStore) CountRelationSyncMutations() (int, error) {
	return 0, fmt.Errorf("CountRelationSyncMutations: not implemented for PostgreSQL")
}

func (s *PostgresStore) ListObservationSyncPayloads() ([]any, error) {
	return nil, fmt.Errorf("ListObservationSyncPayloads: not implemented for PostgreSQL")
}

// PinnedObservations returns pinned observations for a project, newest first.
func (s *PostgresStore) PinnedObservations(project, scope string) ([]Observation, error) {
	project, _ = NormalizeProject(project)
	limit := s.cfg.MaxContextResults
	ctx := context.Background()
	query := `
		SELECT o.id, COALESCE(o.sync_id, '') as sync_id, o.session_id, o.type, o.title, o.content, o.tool_name, o.project,
		       o.scope, o.topic_key, o.revision_count, o.duplicate_count, o.last_seen_at, o.created_at, o.updated_at, o.deleted_at
		FROM observations o
		WHERE o.deleted_at IS NULL AND o.pinned = TRUE`
	args := []any{}
	if project != "" {
		args = append(args, project)
		query += fmt.Sprintf(" AND LOWER(o.project) = $%d", len(args))
	}
	if scope != "" {
		args = append(args, scope)
		query += fmt.Sprintf(" AND o.scope = $%d", len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY o.created_at DESC, o.id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pinned observations: %w", err)
	}
	defer rows.Close()

	var out []Observation
	for rows.Next() {
		obs, err := scanObservationRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, obs)
	}
	return out, rows.Err()
}

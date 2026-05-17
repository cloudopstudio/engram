package store

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ─── FindCandidates ───────────────────────────────────────────────────────────

// FindCandidates runs a post-transaction FTS candidate query for the given
// savedID and returns at most opts.Limit candidates above the score floor.
//
// For each candidate a pending memory_relations row is inserted and the row's
// sync_id is exposed as Candidate.JudgmentID.
//
// Errors from this method are expected to be logged and swallowed by callers —
// detection failure must never fail the originating save.
func (s *PostgresStore) FindCandidates(savedID int64, opts CandidateOptions) ([]Candidate, error) {
	ctx := context.Background()

	limit := opts.Limit
	if limit <= 0 {
		limit = 3
	}
	floor := -2.0
	if opts.BM25Floor != nil {
		floor = *opts.BM25Floor
	}

	var title, project, scope string
	err := s.pool.QueryRow(ctx,
		`SELECT title, COALESCE(project,''), scope FROM observations WHERE id = $1`,
		savedID,
	).Scan(&title, &project, &scope)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("FindCandidates: observation %d not found", savedID)
	}
	if err != nil {
		return nil, fmt.Errorf("FindCandidates: get saved observation: %w", err)
	}

	if opts.Project != "" {
		project = opts.Project
	}
	if opts.Scope != "" {
		scope = opts.Scope
	}

	// Use plainto_tsquery with the engram (unaccent-aware) config so that
	// diacritic variants (código = codigo) match correctly.
	ftsQuery := strings.TrimSpace(title)
	if ftsQuery == "" {
		return nil, nil
	}

	lang := ftsLanguage() // "engram"

	// Fetch up to limit*3 raw candidates from tsvector FTS, then filter by floor.
	ftsSQL := fmt.Sprintf(`
		SELECT o.id, COALESCE(o.sync_id,'') AS sync_id, o.title, o.type, o.topic_key,
		       ts_rank(o.search_vector, q) AS rank
		FROM observations o, plainto_tsquery('%s', $1) q
		WHERE o.search_vector @@ q
		  AND o.id != $2
		  AND o.deleted_at IS NULL
		  AND COALESCE(o.project,'') = $3
		  AND o.scope = $4
		ORDER BY rank DESC
		LIMIT $5
	`, lang)

	rows, err := s.pool.Query(ctx, ftsSQL,
		ftsQuery, savedID, project, scope, limit*3,
	)
	if err != nil {
		return nil, fmt.Errorf("FindCandidates: FTS query: %w", err)
	}
	defer rows.Close()

	type rawCandidate struct {
		id       int64
		syncID   string
		title    string
		obsType  string
		topicKey *string
		score    float64
	}

	var raw []rawCandidate
	for rows.Next() {
		var rc rawCandidate
		if err := rows.Scan(&rc.id, &rc.syncID, &rc.title, &rc.obsType, &rc.topicKey, &rc.score); err != nil {
			return nil, fmt.Errorf("FindCandidates: scan: %w", err)
		}
		// ts_rank returns values in [0,1]; convert to negative BM25-like scale
		// to match the SQLite BM25 floor semantics. We negate so that a higher
		// ts_rank (better match) becomes a value closer to 0 (less negative).
		score := -float64(1.0 - rc.score)
		if score < floor {
			continue
		}
		rc.score = score
		raw = append(raw, rc)
		if len(raw) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("FindCandidates: rows error: %w", err)
	}

	if len(raw) == 0 {
		return nil, nil
	}

	if opts.SkipInsert {
		candidates := make([]Candidate, 0, len(raw))
		for _, rc := range raw {
			candidates = append(candidates, Candidate{
				ID:       rc.id,
				SyncID:   rc.syncID,
				Title:    rc.title,
				Type:     rc.obsType,
				TopicKey: rc.topicKey,
				Score:    rc.score,
			})
		}
		return candidates, nil
	}

	var sourceSyncID string
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(sync_id,'') FROM observations WHERE id = $1`, savedID,
	).Scan(&sourceSyncID); err != nil {
		return nil, fmt.Errorf("FindCandidates: get source sync_id: %w", err)
	}

	candidates := make([]Candidate, 0, len(raw))
	for _, rc := range raw {
		judgmentID := newSyncID("rel")
		_, err := s.pool.Exec(ctx, `
			INSERT INTO memory_relations
				(sync_id, source_id, target_id, relation, judgment_status, created_at, updated_at)
			VALUES ($1, $2, $3, 'pending', 'pending', NOW(), NOW())
			ON CONFLICT (sync_id) DO NOTHING
		`, judgmentID, sourceSyncID, rc.syncID)
		if err != nil {
			log.Printf("[store] FindCandidates: insert pending relation: %v", err)
			continue
		}
		candidates = append(candidates, Candidate{
			ID:         rc.id,
			SyncID:     rc.syncID,
			Title:      rc.title,
			Type:       rc.obsType,
			TopicKey:   rc.topicKey,
			Score:      rc.score,
			JudgmentID: judgmentID,
		})
	}

	return candidates, nil
}

// ─── SaveRelation ─────────────────────────────────────────────────────────────

// SaveRelation inserts a new pending relation row.
func (s *PostgresStore) SaveRelation(p SaveRelationParams) (*Relation, error) {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO memory_relations
			(sync_id, source_id, target_id, relation, judgment_status, created_at, updated_at)
		VALUES ($1, $2, $3, 'pending', 'pending', NOW(), NOW())
	`, p.SyncID, p.SourceID, p.TargetID)
	if err != nil {
		return nil, fmt.Errorf("SaveRelation: insert: %w", err)
	}
	return s.GetRelation(p.SyncID)
}

// ─── GetRelation ──────────────────────────────────────────────────────────────

// GetRelation retrieves a single relation row by its sync_id.
func (s *PostgresStore) GetRelation(syncID string) (*Relation, error) {
	ctx := context.Background()
	return s.scanRelationRow(ctx, s.pool, `
		SELECT id, sync_id,
		       COALESCE(source_id,''), COALESCE(target_id,''),
		       relation, reason, evidence, confidence, judgment_status,
		       marked_by_actor, marked_by_kind, marked_by_model,
		       session_id, created_at, updated_at
		FROM memory_relations
		WHERE sync_id = $1
	`, syncID)
}

// getRelationTxPG is the transactional variant of GetRelation for use within a
// pgx.Tx (e.g. inside JudgeRelation).
func (s *PostgresStore) getRelationTxPG(ctx context.Context, tx pgx.Tx, syncID string) (*Relation, error) {
	return s.scanRelationRow(ctx, tx, `
		SELECT id, sync_id,
		       COALESCE(source_id,''), COALESCE(target_id,''),
		       relation, reason, evidence, confidence, judgment_status,
		       marked_by_actor, marked_by_kind, marked_by_model,
		       session_id, created_at, updated_at
		FROM memory_relations
		WHERE sync_id = $1
	`, syncID)
}

// pgxQuerier is the common interface shared by *pgxpool.Pool and pgx.Tx so
// scanRelationRow can operate on both.
type pgxQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// scanRelationRow scans a single memory_relations row from any pgxQuerier.
func (s *PostgresStore) scanRelationRow(ctx context.Context, q pgxQuerier, query string, args ...any) (*Relation, error) {
	var r Relation
	var sourceID, targetID string
	var createdAt, updatedAt time.Time
	if err := q.QueryRow(ctx, query, args...).Scan(
		&r.ID, &r.SyncID,
		&sourceID, &targetID,
		&r.Relation, &r.Reason, &r.Evidence, &r.Confidence, &r.JudgmentStatus,
		&r.MarkedByActor, &r.MarkedByKind, &r.MarkedByModel,
		&r.SessionID, &createdAt, &updatedAt,
	); err == pgx.ErrNoRows {
		return nil, fmt.Errorf("GetRelation: relation %q not found", args[0])
	} else if err != nil {
		return nil, fmt.Errorf("GetRelation: %w", err)
	}
	r.SourceID = sourceID
	r.TargetID = targetID
	r.CreatedAt = formatTS(createdAt)
	r.UpdatedAt = formatTS(updatedAt)
	return &r, nil
}

// ─── JudgeRelation ────────────────────────────────────────────────────────────

// JudgeRelation records a verdict on an existing pending relation row.
// Re-judge policy: OVERWRITE the existing row.
func (s *PostgresStore) JudgeRelation(p JudgeRelationParams) (*Relation, error) {
	if !isValidRelationVerb(p.Relation) {
		return nil, fmt.Errorf("JudgeRelation: invalid relation verb %q — must be one of: related, compatible, scoped, conflicts_with, supersedes, not_conflict", p.Relation)
	}

	ctx := context.Background()

	var sourceID, targetID string
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(source_id,''), COALESCE(target_id,'') FROM memory_relations WHERE sync_id = $1`,
		p.JudgmentID,
	).Scan(&sourceID, &targetID); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("JudgeRelation: relation %q not found", p.JudgmentID)
		}
		return nil, fmt.Errorf("JudgeRelation: check existence: %w", err)
	}

	var markedByModel *string
	if p.MarkedByModel != "" {
		markedByModel = &p.MarkedByModel
	}
	var sessionID *string
	if p.SessionID != "" {
		sessionID = &p.SessionID
	}

	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		if err := s.validateCrossProjectGuardPG(ctx, tx, sourceID, targetID); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, `
			UPDATE memory_relations
			SET relation        = $1,
			    reason          = $2,
			    evidence        = $3,
			    confidence      = $4,
			    judgment_status = 'judged',
			    marked_by_actor = $5,
			    marked_by_kind  = $6,
			    marked_by_model = $7,
			    session_id      = $8,
			    updated_at      = NOW()
			WHERE sync_id = $9
		`,
			p.Relation,
			p.Reason,
			p.Evidence,
			p.Confidence,
			p.MarkedByActor,
			p.MarkedByKind,
			markedByModel,
			sessionID,
			p.JudgmentID,
		); err != nil {
			return fmt.Errorf("JudgeRelation: update: %w", err)
		}

		// Derive source project for sync mutation enqueue.
		var srcProject string
		_ = tx.QueryRow(ctx,
			`SELECT COALESCE(project,'') FROM observations WHERE sync_id = $1`, sourceID,
		).Scan(&srcProject)
		var tgtProject string
		_ = tx.QueryRow(ctx,
			`SELECT COALESCE(project,'') FROM observations WHERE sync_id = $1`, targetID,
		).Scan(&tgtProject)

		enrollCheckProject := srcProject
		if enrollCheckProject == "" {
			enrollCheckProject = tgtProject
		}
		var enrolled int
		if err := tx.QueryRow(ctx,
			`SELECT 1 FROM sync_enrolled_projects WHERE project = $1 LIMIT 1`, enrollCheckProject,
		).Scan(&enrolled); err != nil && err != pgx.ErrNoRows {
			return fmt.Errorf("JudgeRelation: check enrollment: %w", err)
		}
		if enrolled == 0 {
			return nil
		}

		if srcProject == "" {
			log.Printf("[store] WARNING: JudgeRelation enqueueing relation %s with project='' (source observation missing locally); server will reject", p.JudgmentID)
		}

		rel, err := s.getRelationTxPG(ctx, tx, p.JudgmentID)
		if err != nil {
			return fmt.Errorf("JudgeRelation: read updated relation: %w", err)
		}

		payload := syncRelationPayload{
			SyncID:         rel.SyncID,
			SourceID:       rel.SourceID,
			TargetID:       rel.TargetID,
			Relation:       rel.Relation,
			Reason:         rel.Reason,
			Evidence:       rel.Evidence,
			Confidence:     rel.Confidence,
			JudgmentStatus: rel.JudgmentStatus,
			MarkedByActor:  rel.MarkedByActor,
			MarkedByKind:   rel.MarkedByKind,
			MarkedByModel:  rel.MarkedByModel,
			SessionID:      rel.SessionID,
			Project:        srcProject,
			CreatedAt:      rel.CreatedAt,
			UpdatedAt:      rel.UpdatedAt,
		}
		return s.enqueueSyncMutationTx(ctx, tx, SyncEntityRelation, rel.SyncID, SyncOpUpsert, payload)
	}); err != nil {
		return nil, err
	}

	return s.GetRelation(p.JudgmentID)
}

// ─── Cross-project guard (PG) ─────────────────────────────────────────────────

// validateCrossProjectGuardPG checks that sourceID and targetID belong to the
// same project using the PG transaction. Returns ErrCrossProjectRelation when
// they are in different non-empty projects.
func (s *PostgresStore) validateCrossProjectGuardPG(ctx context.Context, tx pgx.Tx, sourceID, targetID string) error {
	var srcProject, tgtProject string
	_ = tx.QueryRow(ctx,
		`SELECT COALESCE(project,'') FROM observations WHERE sync_id = $1`, sourceID,
	).Scan(&srcProject)
	_ = tx.QueryRow(ctx,
		`SELECT COALESCE(project,'') FROM observations WHERE sync_id = $1`, targetID,
	).Scan(&tgtProject)

	if srcProject != "" && tgtProject != "" && srcProject != tgtProject {
		return ErrCrossProjectRelation
	}
	return nil
}

// ─── JudgeBySemantic ──────────────────────────────────────────────────────────

// JudgeBySemantic persists a semantic verdict into memory_relations.
// When params.Relation is "not_conflict" the call is a no-op.
// Idempotency: upsert on (source_id, target_id) in either direction.
func (s *PostgresStore) JudgeBySemantic(p JudgeBySemanticParams) (string, error) {
	if p.SourceID == "" {
		return "", fmt.Errorf("JudgeBySemantic: SourceID is required")
	}
	if p.TargetID == "" {
		return "", fmt.Errorf("JudgeBySemantic: TargetID is required")
	}
	if !isValidRelationVerb(p.Relation) {
		return "", fmt.Errorf("JudgeBySemantic: invalid relation verb %q — must be one of: related, compatible, scoped, conflicts_with, supersedes, not_conflict", p.Relation)
	}
	if p.Confidence < 0.0 || p.Confidence > 1.0 {
		return "", fmt.Errorf("JudgeBySemantic: confidence %v is out of range [0.0, 1.0]", p.Confidence)
	}

	if p.Relation == RelationNotConflict {
		return "", nil
	}

	ctx := context.Background()
	var resultSyncID string

	if err := s.withTx(ctx, func(tx pgx.Tx) error {
		if err := s.validateCrossProjectGuardPG(ctx, tx, p.SourceID, p.TargetID); err != nil {
			return err
		}

		var existingSyncID string
		err := tx.QueryRow(ctx, `
			SELECT sync_id FROM memory_relations
			WHERE (source_id = $1 AND target_id = $2)
			   OR (source_id = $2 AND target_id = $1)
			LIMIT 1
		`, p.SourceID, p.TargetID).Scan(&existingSyncID)

		var modelPtr *string
		if p.Model != "" {
			modelPtr = &p.Model
		}
		actor := "engram"
		kind := "system"

		if err == pgx.ErrNoRows {
			existingSyncID = newSyncID("rel")
			if _, execErr := tx.Exec(ctx, `
				INSERT INTO memory_relations
					(sync_id, source_id, target_id, relation, judgment_status,
					 confidence, reason,
					 marked_by_actor, marked_by_kind, marked_by_model,
					 created_at, updated_at)
				VALUES ($1, $2, $3, $4, 'judged', $5, $6, $7, $8, $9, NOW(), NOW())
			`, existingSyncID, p.SourceID, p.TargetID, p.Relation,
				p.Confidence, p.Reasoning,
				actor, kind, modelPtr,
			); execErr != nil {
				return fmt.Errorf("JudgeBySemantic: insert: %w", execErr)
			}
		} else if err != nil {
			return fmt.Errorf("JudgeBySemantic: check existing: %w", err)
		} else {
			if _, execErr := tx.Exec(ctx, `
				UPDATE memory_relations
				SET relation        = $1,
				    judgment_status = 'judged',
				    confidence      = $2,
				    reason          = $3,
				    marked_by_actor = $4,
				    marked_by_kind  = $5,
				    marked_by_model = $6,
				    updated_at      = NOW()
				WHERE sync_id = $7
			`, p.Relation, p.Confidence, p.Reasoning,
				actor, kind, modelPtr,
				existingSyncID,
			); execErr != nil {
				return fmt.Errorf("JudgeBySemantic: update: %w", execErr)
			}
		}

		resultSyncID = existingSyncID
		return nil
	}); err != nil {
		return "", err
	}

	return resultSyncID, nil
}

// ─── GetRelationsForObservations ──────────────────────────────────────────────

// GetRelationsForObservations returns a map of observation sync_id →
// ObservationRelations. Relations with judgment_status='orphaned' are excluded.
func (s *PostgresStore) GetRelationsForObservations(syncIDs []string) (map[string]ObservationRelations, error) {
	if len(syncIDs) == 0 {
		return map[string]ObservationRelations{}, nil
	}

	ctx := context.Background()

	// Build $1,$2,... placeholders for PG.
	placeholders := make([]string, len(syncIDs))
	args := make([]any, 0, len(syncIDs)*2)
	for i, id := range syncIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args = append(args, id)
	}
	for _, id := range syncIDs {
		args = append(args, id)
	}
	base := len(syncIDs)
	targetPlaceholders := make([]string, len(syncIDs))
	for i := range syncIDs {
		targetPlaceholders[i] = fmt.Sprintf("$%d", base+i+1)
	}

	inSrc := strings.Join(placeholders, ",")
	inTgt := strings.Join(targetPlaceholders, ",")

	query := fmt.Sprintf(`
		SELECT r.id, r.sync_id,
		       COALESCE(r.source_id,''), COALESCE(r.target_id,''),
		       r.relation, r.reason, r.evidence, r.confidence, r.judgment_status,
		       r.marked_by_actor, r.marked_by_kind, r.marked_by_model,
		       r.session_id, r.created_at, r.updated_at,
		       COALESCE(src.id,0)               AS source_int_id,
		       COALESCE(src.title,'')            AS source_title,
		       (src.id IS NULL OR src.deleted_at IS NOT NULL) AS source_missing,
		       COALESCE(tgt.id,0)               AS target_int_id,
		       COALESCE(tgt.title,'')            AS target_title,
		       (tgt.id IS NULL OR tgt.deleted_at IS NOT NULL) AS target_missing
		FROM memory_relations r
		LEFT JOIN observations src ON src.sync_id = r.source_id
		LEFT JOIN observations tgt ON tgt.sync_id = r.target_id
		WHERE (r.source_id IN (%s) OR r.target_id IN (%s))
		  AND r.judgment_status != 'orphaned'
	`, inSrc, inTgt)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetRelationsForObservations: query: %w", err)
	}
	defer rows.Close()

	result := make(map[string]ObservationRelations)

	for rows.Next() {
		var r Relation
		var sourceID, targetID string
		var createdAt, updatedAt time.Time
		var sourceMissing, targetMissing bool
		if err := rows.Scan(
			&r.ID, &r.SyncID,
			&sourceID, &targetID,
			&r.Relation, &r.Reason, &r.Evidence, &r.Confidence, &r.JudgmentStatus,
			&r.MarkedByActor, &r.MarkedByKind, &r.MarkedByModel,
			&r.SessionID, &createdAt, &updatedAt,
			&r.SourceIntID, &r.SourceTitle, &sourceMissing,
			&r.TargetIntID, &r.TargetTitle, &targetMissing,
		); err != nil {
			return nil, fmt.Errorf("GetRelationsForObservations: scan: %w", err)
		}
		r.SourceID = sourceID
		r.TargetID = targetID
		r.CreatedAt = formatTS(createdAt)
		r.UpdatedAt = formatTS(updatedAt)
		r.SourceMissing = sourceMissing
		r.TargetMissing = targetMissing

		for _, id := range syncIDs {
			if r.SourceID == id {
				entry := result[id]
				entry.AsSource = append(entry.AsSource, r)
				result[id] = entry
			}
			if r.TargetID == id {
				entry := result[id]
				entry.AsTarget = append(entry.AsTarget, r)
				result[id] = entry
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetRelationsForObservations: rows error: %w", err)
	}

	return result, nil
}

// ─── ListRelations / CountRelations ──────────────────────────────────────────

// ListRelations returns a paginated list of relation rows filtered by opts.
func (s *PostgresStore) ListRelations(opts ListRelationsOptions) ([]RelationListItem, error) {
	ctx := context.Background()
	query, args := buildRelationsQueryPG(opts, false)
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ListRelations: query: %w", err)
	}
	defer rows.Close()

	var items []RelationListItem
	for rows.Next() {
		var item RelationListItem
		var createdAt, updatedAt time.Time
		if err := rows.Scan(
			&item.ID, &item.SyncID, &item.Relation, &item.JudgmentStatus,
			&item.SourceID, &item.SourceTitle, &item.TargetID, &item.TargetTitle,
			&createdAt, &updatedAt,
		); err != nil {
			return nil, fmt.Errorf("ListRelations: scan: %w", err)
		}
		item.CreatedAt = formatTS(createdAt)
		item.UpdatedAt = formatTS(updatedAt)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ListRelations: rows error: %w", err)
	}
	if items == nil {
		items = []RelationListItem{}
	}
	return items, nil
}

// CountRelations returns the total number of relation rows matching opts.
func (s *PostgresStore) CountRelations(opts ListRelationsOptions) (int, error) {
	ctx := context.Background()
	query, args := buildRelationsQueryPG(opts, true)
	var total int
	if err := s.pool.QueryRow(ctx, query, args...).Scan(&total); err != nil {
		return 0, fmt.Errorf("CountRelations: %w", err)
	}
	return total, nil
}

// buildRelationsQueryPG builds the SQL for ListRelations and CountRelations (PG dialect).
func buildRelationsQueryPG(opts ListRelationsOptions, countOnly bool) (string, []any) {
	var args []any
	argN := 1

	var selectClause string
	if countOnly {
		selectClause = "SELECT count(*)"
	} else {
		selectClause = `SELECT r.id, r.sync_id, r.relation, r.judgment_status,
			COALESCE(r.source_id,''), COALESCE(src.title,''),
			COALESCE(r.target_id,''), COALESCE(tgt.title,''),
			r.created_at, r.updated_at`
	}

	query := selectClause + `
		FROM memory_relations r
		LEFT JOIN observations src ON src.sync_id = r.source_id AND src.deleted_at IS NULL
		LEFT JOIN observations tgt ON tgt.sync_id = r.target_id AND tgt.deleted_at IS NULL
		WHERE 1=1`

	if opts.Project != "" {
		query += fmt.Sprintf(` AND (COALESCE(src.project,'') = $%d OR COALESCE(tgt.project,'') = $%d)`, argN, argN+1)
		args = append(args, opts.Project, opts.Project)
		argN += 2
	}
	if opts.Status != "" {
		query += fmt.Sprintf(` AND r.judgment_status = $%d`, argN)
		args = append(args, opts.Status)
		argN++
	}
	if !opts.SinceTime.IsZero() {
		query += fmt.Sprintf(` AND r.created_at >= $%d`, argN)
		args = append(args, opts.SinceTime.UTC())
		argN++
	}

	if !countOnly {
		query += ` ORDER BY r.created_at DESC`
		if opts.Limit > 0 {
			query += fmt.Sprintf(` LIMIT $%d`, argN)
			args = append(args, opts.Limit)
			argN++
		}
		if opts.Offset > 0 {
			query += fmt.Sprintf(` OFFSET $%d`, argN)
			args = append(args, opts.Offset)
			argN++
		}
	}

	_ = argN // suppress unused-variable lint
	return query, args
}

// ─── GetRelationStats ─────────────────────────────────────────────────────────

// GetRelationStats returns aggregate counts for a project's relations plus
// the deferred and dead queue totals.
func (s *PostgresStore) GetRelationStats(project string) (RelationStats, error) {
	ctx := context.Background()
	stats := RelationStats{
		Project:          project,
		ByRelation:       map[string]int{},
		ByJudgmentStatus: map[string]int{},
	}

	var q string
	var args []any
	if project != "" {
		q = `
			SELECT r.relation, r.judgment_status, count(*) AS cnt
			FROM memory_relations r
			LEFT JOIN observations src ON src.sync_id = r.source_id AND src.deleted_at IS NULL
			LEFT JOIN observations tgt ON tgt.sync_id = r.target_id AND tgt.deleted_at IS NULL
			WHERE COALESCE(src.project,'') = $1 OR COALESCE(tgt.project,'') = $1
			GROUP BY r.relation, r.judgment_status
		`
		args = []any{project}
	} else {
		q = `
			SELECT relation, judgment_status, count(*) AS cnt
			FROM memory_relations
			GROUP BY relation, judgment_status
		`
	}

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return stats, fmt.Errorf("GetRelationStats: query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var rel, status string
		var cnt int
		if err := rows.Scan(&rel, &status, &cnt); err != nil {
			return stats, fmt.Errorf("GetRelationStats: scan: %w", err)
		}
		stats.ByRelation[rel] += cnt
		stats.ByJudgmentStatus[status] += cnt
	}
	if err := rows.Err(); err != nil {
		return stats, fmt.Errorf("GetRelationStats: rows error: %w", err)
	}

	deferred, dead, err := s.CountDeferredAndDead()
	if err != nil {
		return stats, fmt.Errorf("GetRelationStats: count deferred/dead: %w", err)
	}
	stats.DeferredCount = deferred
	stats.DeadCount = dead

	return stats, nil
}

// ─── CountDeferredAndDead ─────────────────────────────────────────────────────

// CountDeferredAndDead returns the count of rows in sync_apply_deferred by
// apply_status. Only 'deferred' and 'dead' statuses are counted.
func (s *PostgresStore) CountDeferredAndDead() (deferred, dead int, err error) {
	ctx := context.Background()
	rows, err := s.pool.Query(ctx, `
		SELECT apply_status, count(*)
		FROM sync_apply_deferred
		WHERE apply_status IN ('deferred', 'dead')
		GROUP BY apply_status
	`)
	if err != nil {
		return 0, 0, fmt.Errorf("CountDeferredAndDead: query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return 0, 0, fmt.Errorf("CountDeferredAndDead: scan: %w", err)
		}
		switch status {
		case "deferred":
			deferred = n
		case "dead":
			dead = n
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("CountDeferredAndDead: rows error: %w", err)
	}
	return deferred, dead, nil
}

// Compile-time check: *PostgresStore must implement all relation interface methods.
var _ interface {
	FindCandidates(int64, CandidateOptions) ([]Candidate, error)
	SaveRelation(SaveRelationParams) (*Relation, error)
	GetRelation(string) (*Relation, error)
	JudgeRelation(JudgeRelationParams) (*Relation, error)
	JudgeBySemantic(JudgeBySemanticParams) (string, error)
	GetRelationsForObservations([]string) (map[string]ObservationRelations, error)
	ListRelations(ListRelationsOptions) ([]RelationListItem, error)
	CountRelations(ListRelationsOptions) (int, error)
	GetRelationStats(string) (RelationStats, error)
	CountDeferredAndDead() (int, int, error)
} = (*PostgresStore)(nil)

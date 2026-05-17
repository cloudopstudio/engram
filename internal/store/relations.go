package store

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

// ─── Relation vocabulary (locked) ─────────────────────────────────────────────

// Valid relation type values.
const (
	RelationPending       = "pending"
	RelationRelated       = "related"
	RelationCompatible    = "compatible"
	RelationScoped        = "scoped"
	RelationConflictsWith = "conflicts_with"
	RelationSupersedes    = "supersedes"
	RelationNotConflict   = "not_conflict"
)

// Valid judgment_status values.
const (
	JudgmentStatusPending  = "pending"
	JudgmentStatusJudged   = "judged"
	JudgmentStatusOrphaned = "orphaned"
	JudgmentStatusIgnored  = "ignored"
)

// validRelationVerbs is the locked set of relation verbs that mem_judge accepts.
// "pending" is NOT in this set — it is the default, not a verdict.
var validRelationVerbs = map[string]bool{
	RelationRelated:       true,
	RelationCompatible:    true,
	RelationScoped:        true,
	RelationConflictsWith: true,
	RelationSupersedes:    true,
	RelationNotConflict:   true,
}

// isValidRelationVerb returns true if v is an accepted mem_judge relation verb.
func isValidRelationVerb(v string) bool {
	return validRelationVerbs[v]
}

// ─── Types ────────────────────────────────────────────────────────────────────

// CandidateOptions controls the FindCandidates query.
type CandidateOptions struct {
	// Project filters candidates to the same project as the saved observation.
	Project string
	// Scope filters candidates to the same scope as the saved observation.
	Scope string
	// Type is reserved for Phase 2 type-compatibility filtering; NOT enforced Phase 1.
	Type string
	// Limit caps the number of candidates returned. Default 3 when nil or <=0.
	Limit int
	// BM25Floor is the minimum BM25 score (negative; closer to 0 = better match).
	// Candidates below the floor are excluded. Default -2.0 when nil.
	//
	// Use a pointer so that an explicit 0.0 (very strict) is distinguishable
	// from the zero value. nil means "use the default (-2.0)".
	BM25Floor *float64
	// SkipInsert controls whether FindCandidates inserts pending relation rows.
	// When true, candidates are returned but NO rows are written to memory_relations.
	// Default false preserves the existing behaviour (rows are inserted).
	SkipInsert bool
}

// ListRelationsOptions controls ListRelations and CountRelations queries.
type ListRelationsOptions struct {
	// Project filters by the project of the source OR target observation (via JOIN).
	// Empty means no project filter (return all).
	Project string
	// Status filters by judgment_status. Empty means no status filter.
	Status string
	// SinceTime filters to rows created_at >= SinceTime. Zero value means no filter.
	SinceTime time.Time
	// Limit caps the number of rows returned. 0 or negative means no limit.
	Limit int
	// Offset is the pagination offset.
	Offset int
}

// RelationListItem represents a single row in a ListRelations result,
// enriched with observation titles via JOIN (no full Relation struct).
type RelationListItem struct {
	ID             int64  `json:"id"`
	SyncID         string `json:"sync_id"`
	Relation       string `json:"relation"`
	JudgmentStatus string `json:"judgment_status"`
	SourceID       string `json:"source_id"`
	SourceTitle    string `json:"source_title"`
	TargetID       string `json:"target_id"`
	TargetTitle    string `json:"target_title"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

// RelationStats holds aggregate counts of relations for a project.
type RelationStats struct {
	Project          string         `json:"project"`
	ByRelation       map[string]int `json:"by_relation"`
	ByJudgmentStatus map[string]int `json:"by_judgment_status"`
	DeferredCount    int            `json:"deferred"`
	DeadCount        int            `json:"dead"`
}

// Candidate represents a potential conflict candidate surfaced by FindCandidates.
type Candidate struct {
	// ID is the integer primary key of the candidate observation.
	ID int64
	// SyncID is the TEXT sync_id of the candidate observation.
	SyncID string
	// Title is the candidate's title.
	Title string
	// Type is the candidate's observation type.
	Type string
	// TopicKey is the candidate's topic_key (may be nil).
	TopicKey *string
	// Score is the FTS5 BM25 rank (negative; closer to 0 = better match).
	Score float64
	// JudgmentID is the sync_id of the pending memory_relations row created
	// for this (source, candidate) pair.
	JudgmentID string
}

// Relation represents a row in memory_relations.
type Relation struct {
	ID                    int64    `json:"id"`
	SyncID                string   `json:"sync_id"`
	SourceID              string   `json:"source_id"`
	TargetID              string   `json:"target_id"`
	Relation              string   `json:"relation"`
	Reason                *string  `json:"reason,omitempty"`
	Evidence              *string  `json:"evidence,omitempty"`
	Confidence            *float64 `json:"confidence,omitempty"`
	JudgmentStatus        string   `json:"judgment_status"`
	MarkedByActor         *string  `json:"marked_by_actor,omitempty"`
	MarkedByKind          *string  `json:"marked_by_kind,omitempty"`
	MarkedByModel         *string  `json:"marked_by_model,omitempty"`
	SessionID             *string  `json:"session_id,omitempty"`
	CreatedAt             string   `json:"created_at"`
	UpdatedAt             string   `json:"updated_at"`

	// Annotation fields — populated by GetRelationsForObservations via LEFT JOIN.
	// Excluded from JSON output (used only for in-process annotation building).
	SourceIntID   int64  `json:"-"` // integer primary key of source observation
	SourceTitle   string `json:"-"` // title of source observation; empty if missing/deleted
	SourceMissing bool   `json:"-"` // true if source is soft-deleted or not found
	TargetIntID   int64  `json:"-"` // integer primary key of target observation
	TargetTitle   string `json:"-"` // title of target observation; empty if missing/deleted
	TargetMissing bool   `json:"-"` // true if target is soft-deleted or not found
}

// ObservationRelations groups relations for a single observation, split by role.
type ObservationRelations struct {
	// AsSource holds relations where this observation is source_id.
	AsSource []Relation
	// AsTarget holds relations where this observation is target_id.
	AsTarget []Relation
}

// SaveRelationParams holds the inputs for SaveRelation.
type SaveRelationParams struct {
	// SyncID is the unique identifier for this relation row (format: rel-<16hex>).
	SyncID string
	// SourceID is the TEXT sync_id of the source observation.
	SourceID string
	// TargetID is the TEXT sync_id of the target observation.
	TargetID string
}

// JudgeRelationParams holds the inputs for JudgeRelation.
type JudgeRelationParams struct {
	// JudgmentID is the sync_id of the relation row to update (required).
	JudgmentID string
	// Relation is the verdict verb (required); must be one of validRelationVerbs.
	Relation string
	// Reason is an optional free-text explanation.
	Reason *string
	// Evidence is optional free-form JSON or text evidence.
	Evidence *string
	// Confidence is optional 0..1 confidence score.
	Confidence *float64
	// MarkedByActor is the actor identifier (e.g. "agent:claude-sonnet-4-6" or "user").
	MarkedByActor string
	// MarkedByKind is the actor kind ("agent", "human", "system").
	MarkedByKind string
	// MarkedByModel is the model ID (may be empty for human actors).
	MarkedByModel string
	// SessionID is the session in which the judgment was made (optional).
	SessionID string
}

// JudgeBySemanticParams holds the inputs for JudgeBySemantic.
type JudgeBySemanticParams struct {
	// SourceID is the TEXT sync_id of the source observation (required).
	SourceID string
	// TargetID is the TEXT sync_id of the target observation (required).
	TargetID string
	// Relation is the verdict verb (required); must be in validRelationVerbs.
	// Passing "not_conflict" is a no-op: no row is inserted and no error is returned.
	Relation string
	// Confidence is the LLM's self-reported confidence score [0.0, 1.0].
	Confidence float64
	// Reasoning is the LLM's short explanation.
	Reasoning string
	// Model is the LLM model identifier. Stored as marked_by_model.
	Model string
}

// ─── FindCandidates ───────────────────────────────────────────────────────────

// FindCandidates runs a post-transaction FTS5 candidate query for the given
// savedID and returns at most opts.Limit candidates above the BM25 floor.
//
// For each candidate, a pending memory_relations row is inserted and the row's
// sync_id is exposed as Candidate.JudgmentID.
//
// Errors from this method are expected to be logged and swallowed by callers —
// detection failure must never fail the originating save.
func (s *SQLiteStore) FindCandidates(savedID int64, opts CandidateOptions) ([]Candidate, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 3
	}
	floor := -2.0
	if opts.BM25Floor != nil {
		floor = *opts.BM25Floor
	}

	var title, project, scope string
	err := s.db.QueryRow(
		`SELECT title, ifnull(project,''), scope FROM observations WHERE id = ?`, savedID,
	).Scan(&title, &project, &scope)
	if err == sql.ErrNoRows {
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

	ftsQuery := sanitizeFTSCandidates(title)
	if ftsQuery == "" {
		return nil, nil
	}

	rows, err := s.db.Query(`
		SELECT o.id, ifnull(o.sync_id,'') as sync_id, o.title, o.type, o.topic_key,
		       fts.rank
		FROM observations_fts fts
		JOIN observations o ON o.id = fts.rowid
		WHERE observations_fts MATCH ?
		  AND o.id != ?
		  AND o.deleted_at IS NULL
		  AND ifnull(o.project,'') = ifnull(?,'')
		  AND o.scope = ?
		ORDER BY fts.rank
		LIMIT ?
	`, ftsQuery, savedID, project, scope, limit*3)
	if err != nil {
		return nil, fmt.Errorf("FindCandidates: FTS5 query: %w", err)
	}
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
			if closeErr := rows.Close(); closeErr != nil {
				return nil, fmt.Errorf("FindCandidates: scan: %w; close rows: %v", err, closeErr)
			}
			return nil, fmt.Errorf("FindCandidates: scan: %w", err)
		}
		if rc.score < floor {
			continue
		}
		raw = append(raw, rc)
		if len(raw) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		if closeErr := rows.Close(); closeErr != nil {
			return nil, fmt.Errorf("FindCandidates: rows error: %w; close rows: %v", err, closeErr)
		}
		return nil, fmt.Errorf("FindCandidates: rows error: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("FindCandidates: close rows: %w", err)
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
	if err := s.db.QueryRow(
		`SELECT ifnull(sync_id,'') FROM observations WHERE id = ?`, savedID,
	).Scan(&sourceSyncID); err != nil {
		return nil, fmt.Errorf("FindCandidates: get source sync_id: %w", err)
	}

	candidates := make([]Candidate, 0, len(raw))
	for _, rc := range raw {
		judgmentID := newSyncID("rel")
		_, err := s.db.Exec(`
			INSERT INTO memory_relations
				(sync_id, source_id, target_id, relation, judgment_status, created_at, updated_at)
			VALUES (?, ?, ?, 'pending', 'pending', datetime('now'), datetime('now'))
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

// SaveRelation inserts a new pending relation row. The SyncID field must be
// unique (enforced by the UNIQUE constraint on memory_relations.sync_id).
func (s *SQLiteStore) SaveRelation(p SaveRelationParams) (*Relation, error) {
	_, err := s.db.Exec(`
		INSERT INTO memory_relations
			(sync_id, source_id, target_id, relation, judgment_status, created_at, updated_at)
		VALUES (?, ?, ?, 'pending', 'pending', datetime('now'), datetime('now'))
	`, p.SyncID, p.SourceID, p.TargetID)
	if err != nil {
		return nil, fmt.Errorf("SaveRelation: insert: %w", err)
	}
	return s.GetRelation(p.SyncID)
}

// ─── GetRelation ──────────────────────────────────────────────────────────────

// GetRelation retrieves a single relation row by its sync_id.
func (s *SQLiteStore) GetRelation(syncID string) (*Relation, error) {
	row := s.db.QueryRow(`
		SELECT id, sync_id,
		       ifnull(source_id,''), ifnull(target_id,''),
		       relation, reason, evidence, confidence, judgment_status,
		       marked_by_actor, marked_by_kind, marked_by_model,
		       session_id, created_at, updated_at
		FROM memory_relations
		WHERE sync_id = ?
	`, syncID)

	var r Relation
	var sourceID, targetID string
	if err := row.Scan(
		&r.ID, &r.SyncID,
		&sourceID, &targetID,
		&r.Relation, &r.Reason, &r.Evidence, &r.Confidence, &r.JudgmentStatus,
		&r.MarkedByActor, &r.MarkedByKind, &r.MarkedByModel,
		&r.SessionID, &r.CreatedAt, &r.UpdatedAt,
	); err == sql.ErrNoRows {
		return nil, fmt.Errorf("GetRelation: relation %q not found", syncID)
	} else if err != nil {
		return nil, fmt.Errorf("GetRelation: %w", err)
	}
	r.SourceID = sourceID
	r.TargetID = targetID
	return &r, nil
}

// getRelationTx is the transactional variant of GetRelation used within
// JudgeRelation to read the freshly-updated row before commit.
func (s *SQLiteStore) getRelationTx(tx *sql.Tx, syncID string) (*Relation, error) {
	row := tx.QueryRow(`
		SELECT id, sync_id,
		       ifnull(source_id,''), ifnull(target_id,''),
		       relation, reason, evidence, confidence, judgment_status,
		       marked_by_actor, marked_by_kind, marked_by_model,
		       session_id, created_at, updated_at
		FROM memory_relations
		WHERE sync_id = ?
	`, syncID)

	var r Relation
	var sourceID, targetID string
	if err := row.Scan(
		&r.ID, &r.SyncID,
		&sourceID, &targetID,
		&r.Relation, &r.Reason, &r.Evidence, &r.Confidence, &r.JudgmentStatus,
		&r.MarkedByActor, &r.MarkedByKind, &r.MarkedByModel,
		&r.SessionID, &r.CreatedAt, &r.UpdatedAt,
	); err == sql.ErrNoRows {
		return nil, fmt.Errorf("getRelationTx: relation %q not found", syncID)
	} else if err != nil {
		return nil, fmt.Errorf("getRelationTx: %w", err)
	}
	r.SourceID = sourceID
	r.TargetID = targetID
	return &r, nil
}

// ─── JudgeRelation ────────────────────────────────────────────────────────────

// JudgeRelation records a verdict on an existing pending relation row.
//
// Re-judge policy: OVERWRITE the existing row (design decision). The updated
// row is returned on success.
//
// Wraps the UPDATE in a transaction to atomically enqueue a sync mutation when
// the source observation's project is enrolled for cloud sync.
// Returns ErrCrossProjectRelation if source and target belong to different projects.
func (s *SQLiteStore) JudgeRelation(p JudgeRelationParams) (*Relation, error) {
	if !isValidRelationVerb(p.Relation) {
		return nil, fmt.Errorf("JudgeRelation: invalid relation verb %q — must be one of: related, compatible, scoped, conflicts_with, supersedes, not_conflict", p.Relation)
	}

	var sourceID, targetID string
	if err := s.db.QueryRow(
		`SELECT ifnull(source_id,''), ifnull(target_id,'') FROM memory_relations WHERE sync_id = ?`,
		p.JudgmentID,
	).Scan(&sourceID, &targetID); err != nil {
		if err == sql.ErrNoRows {
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

	if err := s.withTx(func(tx *sql.Tx) error {
		if err := validateCrossProjectGuard(tx, sourceID, targetID); err != nil {
			return err
		}

		if _, err := s.execHook(tx, `
			UPDATE memory_relations
			SET relation        = ?,
			    reason          = ?,
			    evidence        = ?,
			    confidence      = ?,
			    judgment_status = 'judged',
			    marked_by_actor = ?,
			    marked_by_kind  = ?,
			    marked_by_model = ?,
			    session_id      = ?,
			    updated_at      = datetime('now')
			WHERE sync_id = ?
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
		_ = tx.QueryRow(
			`SELECT ifnull(project,'') FROM observations WHERE sync_id = ?`, sourceID,
		).Scan(&srcProject)
		var tgtProject string
		_ = tx.QueryRow(
			`SELECT ifnull(project,'') FROM observations WHERE sync_id = ?`, targetID,
		).Scan(&tgtProject)

		enrollCheckProject := srcProject
		if enrollCheckProject == "" {
			enrollCheckProject = tgtProject
		}
		var enrolled int
		if err := tx.QueryRow(
			`SELECT 1 FROM sync_enrolled_projects WHERE project = ? LIMIT 1`, enrollCheckProject,
		).Scan(&enrolled); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("JudgeRelation: check enrollment: %w", err)
		}
		if enrolled == 0 {
			return nil
		}

		if srcProject == "" {
			log.Printf("[store] WARNING: JudgeRelation enqueueing relation %s with project='' (source observation missing locally); server will reject", p.JudgmentID)
		}

		rel, err := s.getRelationTx(tx, p.JudgmentID)
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
		return s.enqueueSyncMutationTx(tx, SyncEntityRelation, rel.SyncID, SyncOpUpsert, payload)
	}); err != nil {
		return nil, err
	}

	return s.GetRelation(p.JudgmentID)
}

// ─── Cross-project guard helper ───────────────────────────────────────────────

// validateCrossProjectGuard checks whether sourceID and targetID belong to the
// same project. It returns ErrCrossProjectRelation when they are in different
// projects. Both empty is allowed (observation may be missing locally).
func validateCrossProjectGuard(tx *sql.Tx, sourceID, targetID string) error {
	var srcProject, tgtProject string
	_ = tx.QueryRow(
		`SELECT ifnull(project,'') FROM observations WHERE sync_id = ?`, sourceID,
	).Scan(&srcProject)
	_ = tx.QueryRow(
		`SELECT ifnull(project,'') FROM observations WHERE sync_id = ?`, targetID,
	).Scan(&tgtProject)

	if srcProject != "" && tgtProject != "" && srcProject != tgtProject {
		return ErrCrossProjectRelation
	}
	return nil
}

// ─── JudgeBySemantic ──────────────────────────────────────────────────────────

// JudgeBySemantic persists a semantic verdict produced by an external agent into
// the memory_relations table with system provenance (marked_by_kind="system",
// marked_by_actor="engram", marked_by_model=params.Model).
//
// When params.Relation is "not_conflict" the call is a no-op: no row is inserted
// and an empty sync_id is returned without error.
//
// Idempotency: if a row already exists for (source_id, target_id) in either
// direction, the existing row is updated (UPSERT). The returned sync_id is
// always the canonical row's sync_id.
func (s *SQLiteStore) JudgeBySemantic(p JudgeBySemanticParams) (string, error) {
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

	var resultSyncID string

	if err := s.withTx(func(tx *sql.Tx) error {
		if err := validateCrossProjectGuard(tx, p.SourceID, p.TargetID); err != nil {
			return err
		}

		var existingSyncID string
		err := tx.QueryRow(`
			SELECT sync_id FROM memory_relations
			WHERE (source_id = ? AND target_id = ?)
			   OR (source_id = ? AND target_id = ?)
			LIMIT 1
		`, p.SourceID, p.TargetID, p.TargetID, p.SourceID).Scan(&existingSyncID)

		confidence := p.Confidence
		var modelPtr *string
		if p.Model != "" {
			modelPtr = &p.Model
		}
		actor := "engram"
		kind := "system"

		if err == sql.ErrNoRows {
			existingSyncID = newSyncID("rel")
			if _, execErr := tx.Exec(`
				INSERT INTO memory_relations
					(sync_id, source_id, target_id, relation, judgment_status,
					 confidence, reason,
					 marked_by_actor, marked_by_kind, marked_by_model,
					 created_at, updated_at)
				VALUES (?, ?, ?, ?, 'judged', ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
			`, existingSyncID, p.SourceID, p.TargetID, p.Relation,
				confidence, p.Reasoning,
				actor, kind, modelPtr,
			); execErr != nil {
				return fmt.Errorf("JudgeBySemantic: insert: %w", execErr)
			}
		} else if err != nil {
			return fmt.Errorf("JudgeBySemantic: check existing: %w", err)
		} else {
			if _, execErr := tx.Exec(`
				UPDATE memory_relations
				SET relation        = ?,
				    judgment_status = 'judged',
				    confidence      = ?,
				    reason          = ?,
				    marked_by_actor = ?,
				    marked_by_kind  = ?,
				    marked_by_model = ?,
				    updated_at      = datetime('now')
				WHERE sync_id = ?
			`, p.Relation, confidence, p.Reasoning,
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
// ObservationRelations for all observations in syncIDs. Relations with
// judgment_status='orphaned' are excluded.
func (s *SQLiteStore) GetRelationsForObservations(syncIDs []string) (map[string]ObservationRelations, error) {
	if len(syncIDs) == 0 {
		return map[string]ObservationRelations{}, nil
	}

	placeholders := make([]string, len(syncIDs))
	args := make([]any, 0, len(syncIDs)*2)
	for i, id := range syncIDs {
		placeholders[i] = "?"
		args = append(args, id)
	}
	for _, id := range syncIDs {
		args = append(args, id)
	}

	inClause := strings.Join(placeholders, ",")
	query := fmt.Sprintf(`
		SELECT r.id, r.sync_id,
		       ifnull(r.source_id,''), ifnull(r.target_id,''),
		       r.relation, r.reason, r.evidence, r.confidence, r.judgment_status,
		       r.marked_by_actor, r.marked_by_kind, r.marked_by_model,
		       r.session_id, r.created_at, r.updated_at,
		       ifnull(src.id,0)              AS source_int_id,
		       ifnull(src.title,'')          AS source_title,
		       (src.id IS NULL OR src.deleted_at IS NOT NULL) AS source_missing,
		       ifnull(tgt.id,0)              AS target_int_id,
		       ifnull(tgt.title,'')          AS target_title,
		       (tgt.id IS NULL OR tgt.deleted_at IS NOT NULL) AS target_missing
		FROM memory_relations r
		LEFT JOIN observations src ON src.sync_id = r.source_id
		LEFT JOIN observations tgt ON tgt.sync_id = r.target_id
		WHERE (r.source_id IN (%s) OR r.target_id IN (%s))
		  AND r.judgment_status != 'orphaned'
	`, inClause, inClause)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetRelationsForObservations: query: %w", err)
	}
	defer rows.Close()

	result := make(map[string]ObservationRelations)

	for rows.Next() {
		var r Relation
		var sourceID, targetID string
		var sourceMissingInt, targetMissingInt int
		if err := rows.Scan(
			&r.ID, &r.SyncID,
			&sourceID, &targetID,
			&r.Relation, &r.Reason, &r.Evidence, &r.Confidence, &r.JudgmentStatus,
			&r.MarkedByActor, &r.MarkedByKind, &r.MarkedByModel,
			&r.SessionID, &r.CreatedAt, &r.UpdatedAt,
			&r.SourceIntID, &r.SourceTitle, &sourceMissingInt,
			&r.TargetIntID, &r.TargetTitle, &targetMissingInt,
		); err != nil {
			return nil, fmt.Errorf("GetRelationsForObservations: scan: %w", err)
		}
		r.SourceID = sourceID
		r.TargetID = targetID
		r.SourceMissing = sourceMissingInt != 0
		r.TargetMissing = targetMissingInt != 0

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

// ─── sanitizeFTSCandidates ────────────────────────────────────────────────────

// sanitizeFTSCandidates builds an OR-based FTS5 query from a title so that
// FindCandidates returns documents with ANY term overlap (not all terms).
func sanitizeFTSCandidates(title string) string {
	words := strings.Fields(title)
	if len(words) == 0 {
		return ""
	}
	quoted := make([]string, 0, len(words))
	for _, w := range words {
		w = strings.Trim(w, `"`)
		if w != "" {
			quoted = append(quoted, `"`+w+`"`)
		}
	}
	return strings.Join(quoted, " OR ")
}

// ─── Phase 3: ListRelations / CountRelations ──────────────────────────────────

// ListRelations returns a paginated list of relation rows filtered by the given options.
func (s *SQLiteStore) ListRelations(opts ListRelationsOptions) ([]RelationListItem, error) {
	query, args := buildRelationsQuery(opts, false)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("ListRelations: query: %w", err)
	}
	defer rows.Close()

	var items []RelationListItem
	for rows.Next() {
		var item RelationListItem
		if err := rows.Scan(
			&item.ID, &item.SyncID, &item.Relation, &item.JudgmentStatus,
			&item.SourceID, &item.SourceTitle, &item.TargetID, &item.TargetTitle,
			&item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("ListRelations: scan: %w", err)
		}
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
func (s *SQLiteStore) CountRelations(opts ListRelationsOptions) (int, error) {
	query, args := buildRelationsQuery(opts, true)
	var total int
	if err := s.db.QueryRow(query, args...).Scan(&total); err != nil {
		return 0, fmt.Errorf("CountRelations: %w", err)
	}
	return total, nil
}

// buildRelationsQuery constructs the SQL for ListRelations and CountRelations.
func buildRelationsQuery(opts ListRelationsOptions, countOnly bool) (string, []any) {
	var args []any

	var selectClause string
	if countOnly {
		selectClause = "SELECT count(*)"
	} else {
		selectClause = `SELECT r.id, r.sync_id, r.relation, r.judgment_status,
			ifnull(r.source_id,''), ifnull(src.title,''),
			ifnull(r.target_id,''), ifnull(tgt.title,''),
			r.created_at, r.updated_at`
	}

	query := selectClause + `
		FROM memory_relations r
		LEFT JOIN observations src ON src.sync_id = r.source_id AND src.deleted_at IS NULL
		LEFT JOIN observations tgt ON tgt.sync_id = r.target_id AND tgt.deleted_at IS NULL
		WHERE 1=1`

	if opts.Project != "" {
		query += ` AND (ifnull(src.project,'') = ? OR ifnull(tgt.project,'') = ?)`
		args = append(args, opts.Project, opts.Project)
	}
	if opts.Status != "" {
		query += ` AND r.judgment_status = ?`
		args = append(args, opts.Status)
	}
	if !opts.SinceTime.IsZero() {
		query += ` AND r.created_at >= ?`
		args = append(args, opts.SinceTime.UTC().Format("2006-01-02T15:04:05Z"))
	}

	if !countOnly {
		query += ` ORDER BY r.created_at DESC`
		if opts.Limit > 0 {
			query += ` LIMIT ?`
			args = append(args, opts.Limit)
		}
		if opts.Offset > 0 {
			query += ` OFFSET ?`
			args = append(args, opts.Offset)
		}
	}

	return query, args
}

// ─── Phase 3: GetRelationStats ─────────────────────────────────────────────────

// GetRelationStats returns aggregate counts for a project's relations plus
// the deferred and dead queue totals.
func (s *SQLiteStore) GetRelationStats(project string) (RelationStats, error) {
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
			WHERE ifnull(src.project,'') = ? OR ifnull(tgt.project,'') = ?
			GROUP BY r.relation, r.judgment_status
		`
		args = []any{project, project}
	} else {
		q = `
			SELECT relation, judgment_status, count(*) AS cnt
			FROM memory_relations
			GROUP BY relation, judgment_status
		`
	}

	rows, err := s.db.Query(q, args...)
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

// CountDeferredAndDead returns the count of rows in sync_apply_deferred grouped
// by apply_status. Only 'deferred' and 'dead' statuses are counted.
func (s *SQLiteStore) CountDeferredAndDead() (deferred, dead int, err error) {
	rows, err := s.db.Query(`
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

// Compile-time check: *SQLiteStore must implement all relation interface methods.
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
} = (*SQLiteStore)(nil)

// Suppress unused import warning for log (used in FindCandidates and JudgeRelation).
var _ = errors.New

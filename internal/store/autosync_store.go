// autosync_store.go — store methods required by internal/cloud/autosync.
//
// These methods extend SQLiteStore with the subset of functionality that the
// autosync Manager needs but which did not exist in the original store:
//
//   - PendingSyncMutationProjectCount type + CountPendingNonEnrolledSyncMutations
//   - MarkSyncBlocked (deterministic blocked state for non-enrolled / paused / auth)
//   - ReplayDeferredResult type + ReplayDeferred (retry deferred relation mutations)
//   - applyRelationUpsertTx (pull-side apply for entity='relation')
//
// NOTE: ApplyPulledMutation in store.go has been updated to delegate relation
// entities to applyRelationUpsertTx and handle FK-miss deferral.
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
)

// ─── PendingSyncMutationProjectCount ─────────────────────────────────────────

// PendingSyncMutationProjectCount groups unenrolled project mutations by project name.
// Returned by CountPendingNonEnrolledSyncMutations so callers can surface per-project
// blocking diagnostics without iterating the full pending queue.
type PendingSyncMutationProjectCount struct {
	Project string `json:"project"`
	Count   int64  `json:"count"`
}

// CountPendingNonEnrolledSyncMutations returns per-project counts of pending
// mutations whose project is not in sync_enrolled_projects. Empty-project
// mutations are always eligible and are excluded from the result.
//
// Autosync uses this to surface "project X is not enrolled" blocking diagnostics
// when ListPendingSyncMutations returns an empty set but unenrolled mutations exist.
func (s *SQLiteStore) CountPendingNonEnrolledSyncMutations(targetKey string) ([]PendingSyncMutationProjectCount, error) {
	targetKey = normalizeSyncTargetKey(targetKey)
	rows, err := s.queryItHook(s.db, `
		SELECT sm.project, COUNT(*)
		FROM sync_mutations sm
		LEFT JOIN sync_enrolled_projects sep ON sm.project = sep.project
		WHERE sm.target_key = ?
		  AND sm.acked_at IS NULL
		  AND sm.project != ''
		  AND sep.project IS NULL
		GROUP BY sm.project
		ORDER BY sm.project ASC`, targetKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := []PendingSyncMutationProjectCount{}
	for rows.Next() {
		var count PendingSyncMutationProjectCount
		if err := rows.Scan(&count.Project, &count.Count); err != nil {
			return nil, err
		}
		counts = append(counts, count)
	}
	return counts, rows.Err()
}

// ─── MarkSyncBlocked ──────────────────────────────────────────────────────────

// MarkSyncBlocked persists a deterministic (non-transient) blocked state.
// Unlike MarkSyncFailure it does NOT increment consecutive_failures and does NOT
// set backoff_until — blocked states are resolved by operator action, not retries.
//
// reasonCode should be one of the constants.Reason* values (e.g. "paused",
// "auth_required", "non_enrolled_pending_mutations").
func (s *SQLiteStore) MarkSyncBlocked(targetKey, reasonCode, message string) error {
	targetKey = normalizeSyncTargetKey(targetKey)
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := s.getSyncStateTx(tx, targetKey); err != nil {
			return err
		}
		_, err := s.execHook(tx,
			`UPDATE sync_state
			 SET lifecycle = ?, consecutive_failures = 0, backoff_until = NULL,
			     reason_code = ?, reason_message = ?, last_error = ?,
			     updated_at = datetime('now')
			 WHERE target_key = ?`,
			SyncLifecycleDegraded, reasonCode, message, message, targetKey,
		)
		return err
	})
}

// ─── ReplayDeferred ──────────────────────────────────────────────────────────

// ReplayDeferredResult holds per-call counts returned by ReplayDeferred.
type ReplayDeferredResult struct {
	Retried   int
	Succeeded int
	Failed    int
	Dead      int
}

// ReplayDeferred retries all rows in sync_apply_deferred with apply_status='deferred'
// (up to 50 per call, ordered by first_seen_at). For each row:
//   - Calls applyRelationUpsertTx inside a transaction.
//   - On success: the apply itself deletes the deferred row (applyRelationUpsertTx
//     includes DELETE FROM sync_apply_deferred on success path).
//   - On ErrRelationFKMissing: increments retry_count; if retry_count reaches 5,
//     marks apply_status='dead'. Otherwise updates last_error + last_attempted_at.
//   - On ErrApplyDead or other decode errors: marks apply_status='dead'.
//
// Dead rows are never retried. Idempotent: calling twice in one cycle does not
// double-retry because successful rows are deleted and failed rows update
// retry_count in place.
func (s *SQLiteStore) ReplayDeferred() (result ReplayDeferredResult, err error) {
	const limit = 50
	const deadThreshold = 5

	rows, err := s.queryItHook(s.db, `
		SELECT sync_id, entity, payload, retry_count
		FROM sync_apply_deferred
		WHERE apply_status = 'deferred'
		ORDER BY first_seen_at
		LIMIT ?
	`, limit)
	if err != nil {
		return result, fmt.Errorf("ReplayDeferred: list deferred: %w", err)
	}

	type deferredRow struct {
		syncID     string
		entity     string
		payload    string
		retryCount int
	}

	var pending []deferredRow
	for rows.Next() {
		var r deferredRow
		if err := rows.Scan(&r.syncID, &r.entity, &r.payload, &r.retryCount); err != nil {
			rows.Close()
			return result, fmt.Errorf("ReplayDeferred: scan: %w", err)
		}
		pending = append(pending, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("ReplayDeferred: rows error: %w", err)
	}

	for _, row := range pending {
		result.Retried++
		mut := SyncMutation{
			Entity:  row.entity,
			Op:      SyncOpUpsert,
			Payload: row.payload,
			Source:  SyncSourceRemote,
		}

		applyErr := s.withTx(func(tx *sql.Tx) error {
			return s.applyRelationUpsertTx(tx, mut)
		})

		if applyErr == nil {
			// Success: applyRelationUpsertTx already deleted the deferred row.
			result.Succeeded++
			log.Printf("[store] replayDeferred: applied sync_id=%s", row.syncID)
			continue
		}

		// Classify the error and update the deferred row.
		newRetry := row.retryCount + 1
		var newStatus string
		if errors.Is(applyErr, ErrRelationFKMissing) && newRetry < deadThreshold {
			// Still retryable.
			newStatus = "deferred"
			result.Failed++
		} else {
			// Either ErrApplyDead, decode error, or retry ceiling reached → dead.
			newStatus = "dead"
			result.Dead++
			log.Printf("[store] replayDeferred: marking dead sync_id=%s err=%v", row.syncID, applyErr)
		}

		if _, updateErr := s.execHook(s.db, `
			UPDATE sync_apply_deferred
			SET apply_status      = ?,
			    retry_count       = ?,
			    last_error        = ?,
			    last_attempted_at = datetime('now')
			WHERE sync_id = ?
		`, newStatus, newRetry, applyErr.Error(), row.syncID); updateErr != nil {
			log.Printf("[store] replayDeferred: failed to update row sync_id=%s: %v", row.syncID, updateErr)
		}
	}

	return result, nil
}

// ─── applyRelationUpsertTx ────────────────────────────────────────────────────

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
func (s *SQLiteStore) applyRelationUpsertTx(tx *sql.Tx, mutation SyncMutation) error {
	// Step 1: decode payload.
	var p syncRelationPayload
	if err := decodeSyncPayload([]byte(mutation.Payload), &p); err != nil {
		return fmt.Errorf("%w: decode relation payload: %v", ErrApplyDead, err)
	}

	// Step 1b: required field validation — missing source_id or target_id is not
	// a retryable FK miss; it is a permanent payload defect (ErrApplyDead).
	if strings.TrimSpace(p.SourceID) == "" || strings.TrimSpace(p.TargetID) == "" {
		return fmt.Errorf("%w: relation payload missing required source_id or target_id", ErrApplyDead)
	}

	// Step 2: FK precondition — both observations must exist locally (by sync_id).
	var obsCount int
	if err := tx.QueryRow(
		`SELECT count(*) FROM observations WHERE sync_id IN (?, ?)`,
		p.SourceID, p.TargetID,
	).Scan(&obsCount); err != nil {
		return fmt.Errorf("applyRelationUpsertTx: check observations: %w", err)
	}
	if obsCount < 2 {
		return ErrRelationFKMissing
	}

	// Step 3: upsert into memory_relations keyed on sync_id (idempotent re-apply).
	if _, err := s.execHook(tx, `
		INSERT INTO memory_relations
			(sync_id, source_id, target_id, relation, reason, evidence, confidence,
			 judgment_status, marked_by_actor, marked_by_kind, marked_by_model,
			 session_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(sync_id) DO UPDATE SET
			source_id       = excluded.source_id,
			target_id       = excluded.target_id,
			relation        = excluded.relation,
			reason          = excluded.reason,
			evidence        = excluded.evidence,
			confidence      = excluded.confidence,
			judgment_status = excluded.judgment_status,
			marked_by_actor = excluded.marked_by_actor,
			marked_by_kind  = excluded.marked_by_kind,
			marked_by_model = excluded.marked_by_model,
			session_id      = excluded.session_id,
			updated_at      = excluded.updated_at
	`,
		p.SyncID, p.SourceID, p.TargetID, p.Relation,
		p.Reason, p.Evidence, p.Confidence,
		p.JudgmentStatus, p.MarkedByActor, p.MarkedByKind, p.MarkedByModel,
		p.SessionID, p.CreatedAt, p.UpdatedAt,
	); err != nil {
		return fmt.Errorf("applyRelationUpsertTx: upsert: %w", err)
	}

	// Step 4: clean up deferred row if one exists (resolves any prior FK-miss deferral).
	if _, err := s.execHook(tx,
		`DELETE FROM sync_apply_deferred WHERE sync_id = ?`, p.SyncID,
	); err != nil {
		return fmt.Errorf("applyRelationUpsertTx: clear deferred: %w", err)
	}

	return nil
}

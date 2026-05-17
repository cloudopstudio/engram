// Package store sentinel errors shared by every backend.
//
// These live in a file without build tags so both the SQLite (store.go)
// and PostgreSQL (store_pg.go) implementations expose the exact same
// error sentinels. Callers can rely on errors.Is across backends.
package store

import "errors"

// Sentinel errors returned by delete operations so callers can use errors.Is.
var (
	ErrSessionNotFound        = errors.New("session not found")
	ErrSessionHasObservations = errors.New("session still has observations")
	ErrPromptNotFound         = errors.New("prompt not found")

	// ErrRelationFKMissing is returned by applyRelationUpsertTx when one or
	// both referenced observations are not present locally (FK precondition not
	// met). The caller defers the mutation to sync_apply_deferred for retry.
	ErrRelationFKMissing = errors.New("relation FK precondition not met: referenced observation missing")

	// ErrCrossProjectRelation is returned by JudgeRelation when the source and
	// target observations belong to different projects. The relation is rejected
	// entirely; no memory_relations row is created and no sync mutation is
	// enqueued.
	ErrCrossProjectRelation = errors.New("relation rejected: source and target observations are in different projects")

	// ErrApplyDead is returned when a deferred relation payload cannot be
	// decoded or is permanently invalid. The caller moves the row to
	// apply_status='dead' in sync_apply_deferred.
	ErrApplyDead = errors.New("relation apply permanently failed: payload invalid or undecodable")
)

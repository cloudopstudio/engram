// Package store sync-related constants shared by every backend.
//
// These live in a file without build tags so both the SQLite (store.go)
// and PostgreSQL (store_pg.go) implementations share the exact same
// constant values for sync lifecycle, entity, op, and source codes.
package store

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

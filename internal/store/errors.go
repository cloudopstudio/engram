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
)

package store

import "time"

// tsFormat is the canonical timestamp layout used to format timestamps stored
// as TEXT in SQLite and PostgreSQL.
const tsFormat = "2006-01-02 15:04:05"

// Now returns the current UTC time formatted with tsFormat. Shared between the
// SQLite and PostgreSQL backends so callers get identical string output.
func Now() string {
	return time.Now().UTC().Format(tsFormat)
}

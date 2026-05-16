// Package store: tiny generic helpers shared by all backends.
//
// Lives in a file without build tags so the SQLite and PostgreSQL backends
// can share these utilities without redeclaration.
package store

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

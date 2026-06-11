package store

import "testing"

func TestNormalizeScopeHandlesGlobal(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"global", "global"},
		{"Global", "global"},
		{"GLOBAL", "global"},
		{"  global  ", "global"},
		{"personal", "personal"},
		{"Personal", "personal"},
		{"project", "project"},
		{"Project", "project"},
		{"", "project"},
		{"unknown", "project"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := normalizeScope(tc.input)
			if got != tc.want {
				t.Errorf("normalizeScope(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

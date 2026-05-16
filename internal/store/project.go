// Package store: project / scope / tool classification helpers shared by all
// backends. These are pure-string operations independent of the database
// driver, so they live in a file without build tags.
package store

import (
	"fmt"
	"strings"
)

// NormalizeProject applies canonical project name normalization:
// lowercase + trim whitespace + collapse consecutive hyphens/underscores.
// Returns the normalized name and a warning message if the name was changed
// (empty string if no change was needed).
// Exported so MCP and CLI handlers can surface the warning to users.
func NormalizeProject(project string) (normalized string, warning string) {
	if project == "" {
		return "", ""
	}
	n := strings.TrimSpace(strings.ToLower(project))
	// Collapse multiple consecutive hyphens
	for strings.Contains(n, "--") {
		n = strings.ReplaceAll(n, "--", "-")
	}
	// Collapse multiple consecutive underscores
	for strings.Contains(n, "__") {
		n = strings.ReplaceAll(n, "__", "_")
	}
	if n == project {
		return n, ""
	}
	return n, fmt.Sprintf("⚠️ Project name normalized: %q → %q", project, n)
}

func normalizeScope(scope string) string {
	v := strings.TrimSpace(strings.ToLower(scope))
	if v == "personal" {
		return "personal"
	}
	return "project"
}

// ClassifyTool returns the observation type for a given tool name.
func ClassifyTool(toolName string) string {
	switch toolName {
	case "write", "edit", "patch":
		return "file_change"
	case "bash":
		return "command"
	case "read", "view":
		return "file_read"
	case "grep", "glob", "ls":
		return "search"
	default:
		return "tool_use"
	}
}

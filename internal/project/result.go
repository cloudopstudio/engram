package project

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"time"
)

// Source constants identify how a project name was resolved.
// These are used as values for DetectionResult.Source to communicate
// the resolution path to callers.
const (
	// SourceConfig means the project was read from a local .engram/config.json file.
	SourceConfig = "config"

	// SourceExplicitOverride means the project was supplied explicitly as an
	// override parameter (e.g. via the project_override tool argument).
	SourceExplicitOverride = "explicit_override"

	// SourceUserSelectedAfterAmbiguousProject means the project was chosen by
	// the user from a list after an ambiguous_project error.
	SourceUserSelectedAfterAmbiguousProject = "user_selected_after_ambiguous_project"

	// SourceSessionProject means the project was inferred from an existing
	// session record in the store.
	SourceSessionProject = "session_project"
)

// ErrAmbiguousProject is returned when cwd is a parent of multiple git
// repositories and the project cannot be determined automatically.
var ErrAmbiguousProject = errors.New("ambiguous project: multiple repositories detected")

// ErrInvalidConfig is returned when a local .engram/config.json file is
// present but cannot be parsed or contains an invalid project name.
var ErrInvalidConfig = errors.New("invalid engram config")

// DetectionResult carries the outcome of project detection, including the
// detected project name, the resolution source, optional path context, and
// any warning or error that occurred during detection.
type DetectionResult struct {
	// Project is the detected (and normalized) project name.
	Project string

	// Source describes how the project was resolved. See the Source* constants.
	Source string

	// Path is the filesystem path associated with the detected project (e.g. git root).
	Path string

	// Warning is a non-fatal diagnostic message, populated when the detection
	// succeeded but a noteworthy condition was observed.
	Warning string

	// Error is set when detection failed or returned a non-fatal structured error.
	Error error

	// AvailableProjects lists candidate project names when detection is ambiguous.
	// Populated only when Error wraps ErrAmbiguousProject.
	AvailableProjects []string
}

// DetectProjectFull is like DetectProject but returns a DetectionResult
// that includes the resolution source and path alongside the project name.
//
// In this fork, project detection is never ambiguous (no multi-repo parent
// detection), so Error and AvailableProjects are always empty.
func DetectProjectFull(dir string) DetectionResult {
	project := DetectProject(dir)
	source := detectSource(dir)
	path := detectPath(dir)
	return DetectionResult{
		Project: project,
		Source:  source,
		Path:    path,
	}
}

// detectSource returns the Source string that best describes how the project
// name would be resolved for the given directory.
func detectSource(dir string) string {
	if dir == "" {
		return "dir"
	}
	if detectFromGitRemote(dir) != "" {
		return "git_remote"
	}
	if detectFromGitRoot(dir) != "" {
		return "git_root"
	}
	return "dir"
}

// detectPath returns the git root path for dir, or dir itself if not in a repo.
func detectPath(dir string) string {
	if dir == "" {
		return ""
	}
	if root := gitRootPath(dir); root != "" {
		return root
	}
	return dir
}

// gitRootPath returns the absolute path of the git root for dir, or empty
// string if dir is not inside a git repository.
func gitRootPath(dir string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

# Changelog

All notable changes to Engram are documented here.

This project follows [Conventional Commits](https://www.conventionalcommits.org/) and uses [GoReleaser](https://goreleaser.com/) to auto-generate GitHub Release notes from commit history on each tag push.

## Where to Find Release Notes

Full release notes with changelogs per version live on the **[GitHub Releases page](https://github.com/Gentleman-Programming/engram/releases)**.

GoReleaser generates them automatically from commits, filtering by type:
- `feat:` / `fix:` / `refactor:` / `chore:` commits appear in the release notes
- `docs:` / `test:` / `ci:` commits are excluded from the generated changelog

## Breaking Changes

Breaking changes are always marked with a `type:breaking-change` label and documented in the release notes with a migration path. The `fix!:` and `feat!:` commit format triggers a major version bump.

## Unreleased

<!-- Changes that are merged but not yet released are tracked here until the next tag. -->

- **refactor(store):** unify SQLite and PostgreSQL into a single binary — both backends are always compiled in, runtime selection via `--db-type` flag or `ENGRAM_DB_TYPE` env var, with auto-detect fallback (PG when `ENGRAM_DATABASE_URL` or a `database-url` profile is configured, otherwise SQLite)
- **feat(cli):** add global `--db-type=sqlite|postgres` flag accepted by every subcommand
- **feat(cli):** `engram login`, `engram aws-login`, `engram migrate` now always present in the binary; they validate the resolved backend at startup and fail with a clear error if it isn't PostgreSQL
- **chore(build):** `-tags pgstore` is no longer required to enable PostgreSQL — every build includes both drivers. The tag remains as a no-op for backward compatibility with old build scripts.
- **feat(project):** add project name auto-detection via git remote and normalization (lowercase + trim + collapse) on all read/write paths
- **feat(cli):** add `engram projects list|consolidate|prune` commands for project hygiene
- **feat(mcp):** add `mem_merge_projects` tool for agent-driven project consolidation
- **feat(mcp):** auto-detect project at MCP startup via `--project` flag, `ENGRAM_PROJECT` env, or git remote
- **feat(mcp):** similar-project warnings when saving to a new project that resembles an existing one
- **fix(sync):** use git remote detection instead of `filepath.Base(cwd)` for project name

### Migration notes (single-binary unification)

- **Before:** `go build -tags pgstore ./cmd/engram && ENGRAM_DATABASE_URL=... ./engram serve`
- **Now (auto-detect):** `go build ./cmd/engram && ENGRAM_DATABASE_URL=... ./engram serve`
- **Now (explicit):** `go build ./cmd/engram && ./engram --db-type=postgres serve`

Existing users who already had `ENGRAM_DATABASE_URL` set or `database-url` in their config profile continue to work unchanged — auto-detect resolves to PostgreSQL exactly as before.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Gentleman-Programming/engram/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	_ "modernc.org/sqlite"
)

const migrateBatchSize = 500

// cmdMigrate migrates data from a local SQLite engram database to PostgreSQL.
// Source: ENGRAM_MIGRATE_SOURCE (default: ~/.engram/engram.db)
// Target: ENGRAM_DATABASE_URL (required)
//
// Usage:
//
//	engram migrate
//	ENGRAM_MIGRATE_SOURCE=/path/to/engram.db engram migrate
func cmdMigrate(cfg store.Config) {
	requirePostgresBackend(cfg, "migrate")

	sourceDB := os.Getenv("ENGRAM_MIGRATE_SOURCE")
	if sourceDB == "" {
		sourceDB = filepath.Join(cfg.DataDir, "engram.db")
	}

	targetURL := os.Getenv("ENGRAM_DATABASE_URL")
	if targetURL == "" {
		fatal(fmt.Errorf("ENGRAM_DATABASE_URL must be set for migration target"))
	}

	// Verify source exists.
	if _, err := os.Stat(sourceDB); os.IsNotExist(err) {
		fatal(fmt.Errorf("source SQLite database not found at %s", sourceDB))
	}

	fmt.Printf("engram migrate — SQLite → PostgreSQL\n")
	fmt.Printf("  Source: %s\n", sourceDB)
	fmt.Printf("  Target: %s (via ENGRAM_DATABASE_URL)\n\n", maskConnStr(targetURL))

	// ── Open source SQLite (read-only) ──
	srcDB, err := sql.Open("sqlite", sourceDB+"?mode=ro")
	if err != nil {
		fatal(fmt.Errorf("open source SQLite: %w", err))
	}
	defer srcDB.Close()
	// Verify connectivity.
	if err := srcDB.Ping(); err != nil {
		fatal(fmt.Errorf("ping source SQLite: %w", err))
	}

	// ── Connect to target PG ──
	ctx := context.Background()

	authMethod := store.ResolveAuthMethodExported(targetURL)
	var ts store.TokenSource
	var identity string
	if authMethod == "entra" {
		tp, err := store.NewTokenProvider()
		if err != nil {
			fatal(fmt.Errorf("entra auth: %w\nSet ENGRAM_AUTH_METHOD=password to use password auth", err))
		}
		if _, err := tp.Token(ctx); err != nil {
			log.Printf("[engram] warning: initial token acquisition failed: %v", err)
		} else {
			identity = tp.Identity()
		}
		ts = tp
	} else if authMethod == "aws-iam" {
		fatal(fmt.Errorf("aws-iam auth is not supported by 'engram migrate' yet — use password auth for migration, then switch to aws-iam in config"))
	}

	pgxCfg, err := store.ConfigurePGPoolExported(targetURL, ts)
	if err != nil {
		fatal(err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	if err != nil {
		fatal(fmt.Errorf("connect to PG: %w", err))
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		fatal(fmt.Errorf("ping PG: %w", err))
	}

	// ── Run PG schema migrations ──
	if err := store.MigratePGExported(pool); err != nil {
		fatal(fmt.Errorf("pg schema migration: %w", err))
	}

	// ── Count source data ──
	var srcSessions, srcObs, srcPrompts int
	srcDB.QueryRow("SELECT COUNT(*) FROM sessions").Scan(&srcSessions)
	srcDB.QueryRow("SELECT COUNT(*) FROM observations").Scan(&srcObs)
	srcDB.QueryRow("SELECT COUNT(*) FROM user_prompts").Scan(&srcPrompts)

	fmt.Printf("Source counts: %d sessions, %d observations, %d prompts\n\n", srcSessions, srcObs, srcPrompts)

	// ── Migrate sessions ──
	sessCount, err := migrateSessions(ctx, srcDB, pool, identity)
	if err != nil {
		fatal(fmt.Errorf("migrate sessions: %w", err))
	}
	fmt.Printf("  Sessions:     %d migrated\n", sessCount)

	// ── Migrate observations ──
	obsCount, err := migrateObservations(ctx, srcDB, pool, identity)
	if err != nil {
		fatal(fmt.Errorf("migrate observations: %w", err))
	}
	fmt.Printf("  Observations: %d migrated\n", obsCount)

	// ── Migrate prompts ──
	promptCount, err := migratePrompts(ctx, srcDB, pool, identity)
	if err != nil {
		fatal(fmt.Errorf("migrate prompts: %w", err))
	}
	fmt.Printf("  Prompts:      %d migrated\n\n", promptCount)

	// ── Validate counts ──
	var pgSessions, pgObs, pgPrompts int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM sessions").Scan(&pgSessions)
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM observations").Scan(&pgObs)
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM user_prompts").Scan(&pgPrompts)

	fmt.Printf("Target counts: %d sessions, %d observations, %d prompts\n", pgSessions, pgObs, pgPrompts)

	if pgSessions >= srcSessions && pgObs >= srcObs && pgPrompts >= srcPrompts {
		fmt.Println("Validation: PASS — counts match or exceed source")
	} else {
		fmt.Println("Validation: WARNING — target counts lower than source")
		fmt.Println("  This may happen if some records were already migrated previously.")
		fmt.Println("  Re-run 'engram migrate' to retry (idempotent).")
	}
}

func migrateSessions(ctx context.Context, srcDB *sql.DB, pool *pgxpool.Pool, identity string) (int, error) {
	rows, err := srcDB.Query("SELECT id, project, directory, started_at, ended_at, summary FROM sessions ORDER BY started_at")
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	batch := &pgx.Batch{}
	batchN := 0

	for rows.Next() {
		var id, project, directory, startedAt string
		var endedAt, summary *string
		if err := rows.Scan(&id, &project, &directory, &startedAt, &endedAt, &summary); err != nil {
			return count, err
		}

		batch.Queue(
			`INSERT INTO sessions (id, project, directory, started_at, ended_at, summary, created_by)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT(id) DO NOTHING`,
			id, project, directory, startedAt, endedAt, summary, identity,
		)
		batchN++

		if batchN >= migrateBatchSize {
			if err := sendBatch(ctx, pool, batch, batchN); err != nil {
				return count, err
			}
			count += batchN
			batch = &pgx.Batch{}
			batchN = 0
		}
	}
	if err := rows.Err(); err != nil {
		return count, err
	}

	if batchN > 0 {
		if err := sendBatch(ctx, pool, batch, batchN); err != nil {
			return count, err
		}
		count += batchN
	}

	return count, nil
}

func migrateObservations(ctx context.Context, srcDB *sql.DB, pool *pgxpool.Pool, identity string) (int, error) {
	rows, err := srcDB.Query(`
		SELECT ifnull(sync_id, ''), session_id, type, title, content, tool_name, project, 
		       ifnull(scope, 'project'), topic_key, normalized_hash,
		       ifnull(revision_count, 1), ifnull(duplicate_count, 1),
		       last_seen_at, created_at, updated_at, deleted_at
		FROM observations ORDER BY id`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	batch := &pgx.Batch{}
	batchN := 0

	for rows.Next() {
		var syncID, sessionID, typ, title, content string
		var toolName, project, scope, topicKey, normHash *string
		var revCount, dupCount int
		var lastSeenAt, createdAt, updatedAt, deletedAt *string

		if err := rows.Scan(
			&syncID, &sessionID, &typ, &title, &content, &toolName, &project,
			&scope, &topicKey, &normHash, &revCount, &dupCount,
			&lastSeenAt, &createdAt, &updatedAt, &deletedAt,
		); err != nil {
			return count, err
		}

		// Ensure sync_id is populated.
		if syncID == "" {
			syncID = store.NewSyncIDExported("obs")
		}

		batch.Queue(
			`INSERT INTO observations (sync_id, session_id, type, title, content, tool_name, project, scope,
			   topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at,
			   created_at, updated_at, deleted_at, created_by)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
			 ON CONFLICT(sync_id) DO NOTHING`,
			syncID, sessionID, typ, title, content, toolName, project, scope,
			topicKey, normHash, revCount, dupCount, lastSeenAt, createdAt, updatedAt, deletedAt, identity,
		)
		batchN++

		if batchN >= migrateBatchSize {
			if err := sendBatch(ctx, pool, batch, batchN); err != nil {
				return count, err
			}
			count += batchN
			if count%5000 == 0 {
				fmt.Printf("  ... %d observations migrated\n", count)
			}
			batch = &pgx.Batch{}
			batchN = 0
		}
	}
	if err := rows.Err(); err != nil {
		return count, err
	}

	if batchN > 0 {
		if err := sendBatch(ctx, pool, batch, batchN); err != nil {
			return count, err
		}
		count += batchN
	}

	return count, nil
}

func migratePrompts(ctx context.Context, srcDB *sql.DB, pool *pgxpool.Pool, identity string) (int, error) {
	rows, err := srcDB.Query(`
		SELECT ifnull(sync_id, ''), session_id, content, ifnull(project, ''), created_at
		FROM user_prompts ORDER BY id`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	batch := &pgx.Batch{}
	batchN := 0

	for rows.Next() {
		var syncID, sessionID, content, project, createdAt string
		if err := rows.Scan(&syncID, &sessionID, &content, &project, &createdAt); err != nil {
			return count, err
		}

		if syncID == "" {
			syncID = store.NewSyncIDExported("prompt")
		}

		batch.Queue(
			`INSERT INTO user_prompts (sync_id, session_id, content, project, created_at, created_by)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT(sync_id) DO NOTHING`,
			syncID, sessionID, content, nullOrString(project), createdAt, identity,
		)
		batchN++

		if batchN >= migrateBatchSize {
			if err := sendBatch(ctx, pool, batch, batchN); err != nil {
				return count, err
			}
			count += batchN
			batch = &pgx.Batch{}
			batchN = 0
		}
	}
	if err := rows.Err(); err != nil {
		return count, err
	}

	if batchN > 0 {
		if err := sendBatch(ctx, pool, batch, batchN); err != nil {
			return count, err
		}
		count += batchN
	}

	return count, nil
}

func sendBatch(ctx context.Context, pool *pgxpool.Pool, batch *pgx.Batch, n int) error {
	results := pool.SendBatch(ctx, batch)
	defer results.Close()
	for i := 0; i < n; i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("batch item %d: %w", i, err)
		}
	}
	return nil
}

func nullOrString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// maskConnStr hides password from connection string for display.
func maskConnStr(s string) string {
	// Naive: replace :password@ with :****@
	if idx := len(s); idx > 0 {
		// Just show the host portion
		for i, c := range s {
			if c == '@' {
				return "postgres://****@" + s[i+1:]
			}
		}
	}
	return s
}

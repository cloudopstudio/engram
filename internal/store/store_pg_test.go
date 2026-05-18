package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

// ─── Test Infrastructure ─────────────────────────────────────────────────────

func newTestStorePG(t *testing.T) *PostgresStore {
	t.Helper()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	pool.MaxWait = 30 * time.Second

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16-alpine",
		Env: []string{
			"POSTGRES_DB=engram_test",
			"POSTGRES_USER=engram",
			"POSTGRES_PASSWORD=test",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		t.Skipf("Could not start PG container: %v", err)
	}
	t.Cleanup(func() { pool.Purge(resource) })

	connStr := fmt.Sprintf("postgres://engram:test@localhost:%s/engram_test?sslmode=disable",
		resource.GetPort("5432/tcp"))

	var pgPool *pgxpool.Pool
	if err := pool.Retry(func() error {
		var err error
		pgPool, err = pgxpool.New(context.Background(), connStr)
		if err != nil {
			return err
		}
		return pgPool.Ping(context.Background())
	}); err != nil {
		t.Fatalf("pg not ready: %v", err)
	}

	if err := migratePG(pgPool); err != nil {
		t.Fatalf("pg migration: %v", err)
	}

	cfg := Config{
		DataDir:              t.TempDir(),
		MaxObservationLength: 50000,
		MaxContextResults:    20,
		MaxSearchResults:     20,
		DedupeWindow:         time.Hour,
	}

	return &PostgresStore{pool: pgPool, cfg: cfg, identity: "test@example.com"}
}

// ─── Session CRUD Tests ──────────────────────────────────────────────────────

func TestPGCreateSessionAndGet(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	err := s.CreateSession("sess-1", "myproject", "/path/to/dir")
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	sess, err := s.GetSession("sess-1")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if sess.ID != "sess-1" || sess.Project != "myproject" || sess.Directory != "/path/to/dir" {
		t.Fatalf("unexpected session: %+v", sess)
	}
	if sess.StartedAt == "" {
		t.Fatal("expected started_at to be populated")
	}
}

func TestPGCreateSessionUpsert(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj-a", "/dir-a")
	// Re-create with new values — should NOT overwrite existing non-empty fields.
	s.CreateSession("sess-1", "proj-b", "/dir-b")

	sess, _ := s.GetSession("sess-1")
	if sess.Project != "proj-a" {
		t.Fatalf("expected project to remain 'proj-a', got %q", sess.Project)
	}

	// But if original was empty, it should update.
	s.CreateSession("sess-2", "", "")
	s.CreateSession("sess-2", "filled", "/filled")
	sess2, _ := s.GetSession("sess-2")
	if sess2.Project != "filled" {
		t.Fatalf("expected project to be filled, got %q", sess2.Project)
	}
}

func TestPGEndSession(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	err := s.EndSession("sess-1", "All done")
	if err != nil {
		t.Fatalf("EndSession: %v", err)
	}

	sess, _ := s.GetSession("sess-1")
	if sess.EndedAt == nil {
		t.Fatal("expected ended_at to be set")
	}
	if sess.Summary == nil || *sess.Summary != "All done" {
		t.Fatal("expected summary to be 'All done'")
	}

	// End non-existent session — should be no-op.
	err = s.EndSession("nonexistent", "nope")
	if err != nil {
		t.Fatalf("EndSession on nonexistent should not error: %v", err)
	}
}

func TestPGRecentSessions(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("sess-%d", i)
		s.CreateSession(id, "proj", "/dir")
	}

	results, err := s.RecentSessions("", 5)
	if err != nil {
		t.Fatalf("RecentSessions: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 sessions, got %d", len(results))
	}
}

func TestPGAllSessions(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("s1", "proj-a", "/a")
	s.CreateSession("s2", "proj-b", "/b")

	all, err := s.AllSessions("proj-a", 10)
	if err != nil {
		t.Fatalf("AllSessions: %v", err)
	}
	if len(all) != 1 || all[0].ID != "s1" {
		t.Fatalf("expected 1 session for proj-a, got %d", len(all))
	}
}

// ─── Observation CRUD Tests ──────────────────────────────────────────────────

func TestPGAddAndGetObservation(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	id, err := s.AddObservation(AddObservationParams{
		SessionID: "sess-1",
		Type:      "decision",
		Title:     "Use JWT for auth",
		Content:   "We decided to use JWT tokens for authentication",
		Project:   "proj",
		Scope:     "project",
	})
	if err != nil {
		t.Fatalf("AddObservation: %v", err)
	}
	if id <= 0 {
		t.Fatal("expected positive ID")
	}

	obs, err := s.GetObservation(id)
	if err != nil {
		t.Fatalf("GetObservation: %v", err)
	}
	if obs.Title != "Use JWT for auth" || obs.Type != "decision" {
		t.Fatalf("unexpected observation: %+v", obs)
	}
	if obs.SyncID == "" {
		t.Fatal("expected sync_id to be populated")
	}
}

func TestPGAddObservationTopicKeyUpsert(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")

	id1, _ := s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "Auth v1",
		Content: "Version 1", Project: "proj", TopicKey: "architecture/auth",
	})

	id2, _ := s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "Auth v2",
		Content: "Version 2", Project: "proj", TopicKey: "architecture/auth",
	})

	if id1 != id2 {
		t.Fatalf("expected same ID (upsert), got %d and %d", id1, id2)
	}

	obs, _ := s.GetObservation(id1)
	if obs.Content != "Version 2" {
		t.Fatalf("expected updated content, got %q", obs.Content)
	}
	if obs.RevisionCount != 2 {
		t.Fatalf("expected revision_count=2, got %d", obs.RevisionCount)
	}
}

func TestPGAddObservationDedup(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")

	id1, _ := s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "Same title",
		Content: "Same content", Project: "proj",
	})
	id2, _ := s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "Same title",
		Content: "Same content", Project: "proj",
	})

	if id1 != id2 {
		t.Fatalf("expected dedup (same ID), got %d and %d", id1, id2)
	}

	obs, _ := s.GetObservation(id1)
	if obs.DuplicateCount != 2 {
		t.Fatalf("expected duplicate_count=2, got %d", obs.DuplicateCount)
	}
}

func TestPGAddObservationStripsPrivateTags(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	id, _ := s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "manual", Title: "Test",
		Content: "Public data <private>secret key: abc123</private> more public",
		Project: "proj",
	})

	obs, _ := s.GetObservation(id)
	if strings.Contains(obs.Content, "secret") || strings.Contains(obs.Content, "abc123") {
		t.Fatal("private content should be stripped")
	}
	if !strings.Contains(obs.Content, "[REDACTED]") {
		t.Fatal("expected [REDACTED] placeholder")
	}
}

func TestPGUpdateObservation(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	id, _ := s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "Original",
		Content: "Original content", Project: "proj",
	})

	newTitle := "Updated title"
	updated, err := s.UpdateObservation(id, UpdateObservationParams{Title: &newTitle})
	if err != nil {
		t.Fatalf("UpdateObservation: %v", err)
	}
	if updated.Title != "Updated title" {
		t.Fatalf("expected updated title, got %q", updated.Title)
	}
	if updated.RevisionCount != 2 {
		t.Fatalf("expected revision_count=2, got %d", updated.RevisionCount)
	}
}

func TestPGDeleteObservationSoft(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	id, _ := s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "manual", Title: "To delete",
		Content: "Content", Project: "proj",
	})

	err := s.DeleteObservation(id, false)
	if err != nil {
		t.Fatalf("DeleteObservation soft: %v", err)
	}

	// GetObservation should now fail (soft deleted = excluded by WHERE deleted_at IS NULL).
	_, err = s.GetObservation(id)
	if err == nil {
		t.Fatal("expected error getting soft-deleted observation")
	}
}

func TestPGDeleteObservationHard(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	id, _ := s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "manual", Title: "To hard delete",
		Content: "Content", Project: "proj",
	})

	err := s.DeleteObservation(id, true)
	if err != nil {
		t.Fatalf("DeleteObservation hard: %v", err)
	}

	_, err = s.GetObservation(id)
	if err != pgx.ErrNoRows {
		t.Fatalf("expected ErrNoRows after hard delete, got %v", err)
	}
}

func TestPGAllObservationsAndRecent(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	for i := 0; i < 5; i++ {
		s.AddObservation(AddObservationParams{
			SessionID: "sess-1", Type: "manual", Title: fmt.Sprintf("Obs %d", i),
			Content: fmt.Sprintf("Content %d", i), Project: "proj",
		})
	}

	all, err := s.AllObservations("proj", "", 10)
	if err != nil {
		t.Fatalf("AllObservations: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("expected 5 observations, got %d", len(all))
	}

	recent, err := s.RecentObservations("proj", "project", 3)
	if err != nil {
		t.Fatalf("RecentObservations: %v", err)
	}
	if len(recent) != 3 {
		t.Fatalf("expected 3 recent, got %d", len(recent))
	}
}

func TestPGSessionObservations(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	s.CreateSession("sess-2", "proj", "/dir")

	s.AddObservation(AddObservationParams{SessionID: "sess-1", Type: "manual", Title: "A", Content: "a", Project: "proj"})
	s.AddObservation(AddObservationParams{SessionID: "sess-2", Type: "manual", Title: "B", Content: "b", Project: "proj"})

	obs, _ := s.SessionObservations("sess-1", 10)
	if len(obs) != 1 || obs[0].Title != "A" {
		t.Fatalf("expected 1 observation for sess-1, got %d", len(obs))
	}
}

// ─── Prompt Tests ────────────────────────────────────────────────────────────

func TestPGAddPromptAndRecent(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	id, err := s.AddPrompt(AddPromptParams{
		SessionID: "sess-1", Content: "How do I fix auth?", Project: "proj",
	})
	if err != nil {
		t.Fatalf("AddPrompt: %v", err)
	}
	if id <= 0 {
		t.Fatal("expected positive ID")
	}

	prompts, _ := s.RecentPrompts("proj", 10)
	if len(prompts) != 1 || prompts[0].Content != "How do I fix auth?" {
		t.Fatalf("unexpected prompts: %+v", prompts)
	}
}

func TestPGSearchPrompts(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddPrompt(AddPromptParams{SessionID: "sess-1", Content: "How to configure JWT authentication", Project: "proj"})
	s.AddPrompt(AddPromptParams{SessionID: "sess-1", Content: "What are the best React patterns", Project: "proj"})

	results, err := s.SearchPrompts("authentication", "", 10)
	if err != nil {
		t.Fatalf("SearchPrompts: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one result for 'authentication'")
	}
	if !strings.Contains(results[0].Content, "JWT") {
		t.Fatal("expected JWT prompt to be first result")
	}
}

// ─── FTS Search Tests ────────────────────────────────────────────────────────

func TestPGSearchFreeText(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "JWT authentication",
		Content: "We decided to use JWT for authentication", Project: "proj",
	})
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "bugfix", Title: "Fixed timeout",
		Content: "Increased connection timeout to 30s", Project: "proj",
	})

	results, err := s.Search("authentication", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results for 'authentication'")
	}
	if results[0].Title != "JWT authentication" {
		t.Fatalf("expected JWT observation first, got %q", results[0].Title)
	}
}

func TestPGSearchTopicKey(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "Auth model",
		Content: "Auth architecture decision", Project: "proj", TopicKey: "architecture/auth-model",
	})

	results, _ := s.Search("architecture/auth-model", SearchOptions{})
	if len(results) == 0 {
		t.Fatal("expected topic_key direct lookup result")
	}
	if results[0].Rank != -1000 {
		t.Fatalf("expected rank=-1000 for topic_key result, got %f", results[0].Rank)
	}
}

func TestPGSearchEmptyQuery(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	results, err := s.Search("", SearchOptions{})
	if err != nil {
		t.Fatalf("Search empty query should not error: %v", err)
	}
	if results != nil {
		t.Fatal("expected nil results for empty query")
	}
}

func TestPGSearchFilters(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj-a", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "Auth in proj-a",
		Content: "Authentication decision for project A", Project: "proj-a",
	})
	s.CreateSession("sess-2", "proj-b", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-2", Type: "bugfix", Title: "Auth fix in proj-b",
		Content: "Fixed authentication bug in project B", Project: "proj-b",
	})

	results, _ := s.Search("authentication", SearchOptions{Project: "proj-a", Limit: 10})
	if len(results) != 1 {
		t.Fatalf("expected 1 result filtered to proj-a, got %d", len(results))
	}

	results2, _ := s.Search("authentication", SearchOptions{Type: "bugfix", Limit: 10})
	if len(results2) != 1 || results2[0].Type != "bugfix" {
		t.Fatalf("expected 1 bugfix result, got %d", len(results2))
	}
}

func TestPGSearchStemming(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "architectural decision",
		Content: "We decided to use hexagonal architecture", Project: "proj",
	})

	// "deciding" should match "decided" via stemming.
	results, err := s.Search("deciding", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Search stemming: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected stemming to match 'deciding' with 'decided'")
	}
}

func TestPGSearchWeightedRanking(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	// Observation A: "authentication" in title (weight A).
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "authentication middleware",
		Content: "Implemented a middleware layer", Project: "proj",
	})
	// Observation B: "authentication" only in content (weight B).
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "bugfix", Title: "Fixed timeout bug",
		Content: "Fixed authentication timeout issue", Project: "proj",
	})

	results, _ := s.Search("authentication", SearchOptions{Limit: 10})
	if len(results) < 2 {
		t.Fatalf("expected at least 2 results, got %d", len(results))
	}
	// Title match should rank higher.
	if results[0].Title != "authentication middleware" {
		t.Fatalf("expected title-match first, got %q", results[0].Title)
	}
}

func TestPGSearchSpecialChars(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "bugfix", Title: "C++ error fix",
		Content: "Fixed undefined reference error in C++ module", Project: "proj",
	})

	// Should not crash PG.
	_, err := s.Search(`C++ error: undefined (reference)`, SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Search with special chars should not error: %v", err)
	}
}

func TestPGSearchDeduplicatesTopicKeyAndFTS(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "decision", Title: "Auth model decision",
		Content: "Decided on JWT auth model", Project: "proj", TopicKey: "decision/auth-model",
	})

	// Query that matches both topic_key (contains /) and FTS.
	results, _ := s.Search("decision/auth-model", SearchOptions{Limit: 10})
	// Count occurrences of the observation.
	ids := map[int64]int{}
	for _, r := range results {
		ids[r.ID]++
	}
	for id, count := range ids {
		if count > 1 {
			t.Fatalf("observation %d appeared %d times (should be deduplicated)", id, count)
		}
	}
}

// ─── Timeline Tests ──────────────────────────────────────────────────────────

func TestPGTimeline(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	var ids []int64
	for i := 0; i < 10; i++ {
		id, _ := s.AddObservation(AddObservationParams{
			SessionID: "sess-1", Type: "manual", Title: fmt.Sprintf("Obs %d", i),
			Content: fmt.Sprintf("Content %d", i), Project: "proj",
		})
		ids = append(ids, id)
	}

	// Timeline around the 5th observation.
	result, err := s.Timeline(ids[4], 2, 2)
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if result.Focus.ID != ids[4] {
		t.Fatalf("expected focus ID %d, got %d", ids[4], result.Focus.ID)
	}
	if len(result.Before) != 2 {
		t.Fatalf("expected 2 before entries, got %d", len(result.Before))
	}
	if len(result.After) != 2 {
		t.Fatalf("expected 2 after entries, got %d", len(result.After))
	}
	// Before should be in chronological order (oldest first).
	if result.Before[0].ID >= result.Before[1].ID {
		t.Fatal("before entries should be in chronological order")
	}
	if result.TotalInRange != 10 {
		t.Fatalf("expected total_in_range=10, got %d", result.TotalInRange)
	}
}

// ─── Stats & Context Tests ───────────────────────────────────────────────────

func TestPGStats(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj-a", "/dir")
	s.CreateSession("sess-2", "proj-b", "/dir")
	s.AddObservation(AddObservationParams{SessionID: "sess-1", Type: "manual", Title: "A", Content: "a", Project: "proj-a"})
	s.AddObservation(AddObservationParams{SessionID: "sess-2", Type: "manual", Title: "B", Content: "b", Project: "proj-b"})
	s.AddPrompt(AddPromptParams{SessionID: "sess-1", Content: "Hello", Project: "proj-a"})

	stats, err := s.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.TotalSessions != 2 {
		t.Fatalf("expected 2 sessions, got %d", stats.TotalSessions)
	}
	if stats.TotalObservations != 2 {
		t.Fatalf("expected 2 observations, got %d", stats.TotalObservations)
	}
	if stats.TotalPrompts != 1 {
		t.Fatalf("expected 1 prompt, got %d", stats.TotalPrompts)
	}
	if len(stats.Projects) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(stats.Projects))
	}
}

func TestPGFormatContext(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Empty context.
	ctx, _ := s.FormatContext("proj", "")
	if ctx != "" {
		t.Fatal("expected empty context for no data")
	}

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{SessionID: "sess-1", Type: "decision", Title: "Auth", Content: "JWT decision", Project: "proj"})
	s.AddPrompt(AddPromptParams{SessionID: "sess-1", Content: "How to fix auth?", Project: "proj"})

	ctx, err := s.FormatContext("proj", "")
	if err != nil {
		t.Fatalf("FormatContext: %v", err)
	}
	if !strings.Contains(ctx, "Memory from Previous Sessions") {
		t.Fatal("expected context header")
	}
	if !strings.Contains(ctx, "Auth") {
		t.Fatal("expected observation title in context")
	}
}

// ─── Export/Import Tests ─────────────────────────────────────────────────────

func TestPGExportImport(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{SessionID: "sess-1", Type: "decision", Title: "A", Content: "Content A", Project: "proj"})
	s.AddPrompt(AddPromptParams{SessionID: "sess-1", Content: "Hello", Project: "proj"})

	data, err := s.Export()
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if len(data.Sessions) != 1 || len(data.Observations) != 1 || len(data.Prompts) != 1 {
		t.Fatalf("unexpected export: %d sessions, %d obs, %d prompts",
			len(data.Sessions), len(data.Observations), len(data.Prompts))
	}

	// Import into a fresh store (same PG, different data scope).
	s2 := newTestStorePG(t)
	defer s2.Close()

	result, err := s2.Import(data)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if result.SessionsImported != 1 || result.ObservationsImported != 1 || result.PromptsImported != 1 {
		t.Fatalf("unexpected import counts: %+v", result)
	}

	// Re-import should be idempotent for sessions (ON CONFLICT DO NOTHING).
	result2, err := s2.Import(data)
	if err != nil {
		t.Fatalf("Re-import: %v", err)
	}
	if result2.SessionsImported != 0 {
		t.Fatalf("expected 0 sessions on re-import, got %d", result2.SessionsImported)
	}
}

// ─── Sync State Tests ────────────────────────────────────────────────────────

func TestPGSyncState(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	state, err := s.GetSyncState("cloud")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if state.Lifecycle != SyncLifecycleIdle {
		t.Fatalf("expected idle lifecycle, got %q", state.Lifecycle)
	}
}

func TestPGSyncMutationsEnqueuedOnWrite(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Enroll the project so sync mutations are visible via ListPendingSyncMutations.
	if err := s.EnrollProject("proj"); err != nil {
		t.Fatalf("EnrollProject: %v", err)
	}

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "manual", Title: "Test", Content: "Content", Project: "proj",
	})

	// There should be sync mutations for the session and observation.
	mutations, err := s.ListPendingSyncMutations("cloud", 100)
	if err != nil {
		t.Fatalf("ListPendingSyncMutations: %v", err)
	}
	if len(mutations) < 2 {
		t.Fatalf("expected at least 2 mutations (session + observation), got %d", len(mutations))
	}
}

func TestPGAckSyncMutations(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Enroll the project so sync mutations are visible via ListPendingSyncMutations.
	if err := s.EnrollProject("proj"); err != nil {
		t.Fatalf("EnrollProject: %v", err)
	}

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "manual", Title: "Test", Content: "Content", Project: "proj",
	})

	mutations, _ := s.ListPendingSyncMutations("cloud", 100)
	if len(mutations) == 0 {
		t.Fatal("expected mutations after enroll + write, got 0")
	}
	maxSeq := mutations[len(mutations)-1].Seq

	err := s.AckSyncMutations("cloud", maxSeq)
	if err != nil {
		t.Fatalf("AckSyncMutations: %v", err)
	}

	remaining, _ := s.ListPendingSyncMutations("cloud", 100)
	if len(remaining) != 0 {
		t.Fatalf("expected 0 pending after ack, got %d", len(remaining))
	}

	state, _ := s.GetSyncState("cloud")
	if state.Lifecycle != SyncLifecycleHealthy {
		t.Fatalf("expected healthy after full ack, got %q", state.Lifecycle)
	}
}

func TestPGAckSyncMutationSeqs(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Enroll the project so sync mutations are visible via ListPendingSyncMutations.
	if err := s.EnrollProject("proj"); err != nil {
		t.Fatalf("EnrollProject: %v", err)
	}

	s.CreateSession("sess-1", "proj", "/dir")
	s.AddObservation(AddObservationParams{SessionID: "sess-1", Type: "manual", Title: "A", Content: "a", Project: "proj"})
	s.AddObservation(AddObservationParams{SessionID: "sess-1", Type: "manual", Title: "B", Content: "b", Project: "proj"})

	mutations, _ := s.ListPendingSyncMutations("cloud", 100)
	if len(mutations) == 0 {
		t.Fatal("expected mutations after enroll + write, got 0")
	}
	// Ack only the first mutation.
	err := s.AckSyncMutationSeqs("cloud", []int64{mutations[0].Seq})
	if err != nil {
		t.Fatalf("AckSyncMutationSeqs: %v", err)
	}

	remaining, _ := s.ListPendingSyncMutations("cloud", 100)
	if len(remaining) != len(mutations)-1 {
		t.Fatalf("expected %d remaining, got %d", len(mutations)-1, len(remaining))
	}
}

func TestPGSyncLease(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	now := time.Now().UTC()
	acquired, err := s.AcquireSyncLease("cloud", "owner-1", time.Minute, now)
	if err != nil {
		t.Fatalf("AcquireSyncLease: %v", err)
	}
	if !acquired {
		t.Fatal("expected lease to be acquired")
	}

	// Second owner should be denied.
	acquired2, _ := s.AcquireSyncLease("cloud", "owner-2", time.Minute, now)
	if acquired2 {
		t.Fatal("expected second owner to be denied")
	}

	// Same owner can re-acquire.
	acquired3, _ := s.AcquireSyncLease("cloud", "owner-1", time.Minute, now)
	if !acquired3 {
		t.Fatal("expected same owner to re-acquire")
	}

	// Release.
	err = s.ReleaseSyncLease("cloud", "owner-1")
	if err != nil {
		t.Fatalf("ReleaseSyncLease: %v", err)
	}

	// Now second owner can acquire.
	acquired4, _ := s.AcquireSyncLease("cloud", "owner-2", time.Minute, now)
	if !acquired4 {
		t.Fatal("expected owner-2 to acquire after release")
	}
}

func TestPGMarkSyncFailureAndHealthy(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	err := s.MarkSyncFailure("cloud", "connection refused", time.Now().Add(5*time.Minute))
	if err != nil {
		t.Fatalf("MarkSyncFailure: %v", err)
	}

	state, _ := s.GetSyncState("cloud")
	if state.Lifecycle != SyncLifecycleDegraded {
		t.Fatalf("expected degraded, got %q", state.Lifecycle)
	}
	if state.ConsecutiveFailures != 1 {
		t.Fatalf("expected 1 failure, got %d", state.ConsecutiveFailures)
	}

	err = s.MarkSyncHealthy("cloud")
	if err != nil {
		t.Fatalf("MarkSyncHealthy: %v", err)
	}

	state2, _ := s.GetSyncState("cloud")
	if state2.Lifecycle != SyncLifecycleHealthy || state2.ConsecutiveFailures != 0 {
		t.Fatalf("expected healthy with 0 failures, got %q/%d", state2.Lifecycle, state2.ConsecutiveFailures)
	}
}

func TestPGSyncChunks(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	err := s.RecordSyncedChunk("chunk-1")
	if err != nil {
		t.Fatalf("RecordSyncedChunk: %v", err)
	}
	// Idempotent.
	err = s.RecordSyncedChunk("chunk-1")
	if err != nil {
		t.Fatalf("RecordSyncedChunk idempotent: %v", err)
	}

	chunks, err := s.GetSyncedChunks()
	if err != nil {
		t.Fatalf("GetSyncedChunks: %v", err)
	}
	if !chunks["chunk-1"] {
		t.Fatal("expected chunk-1 to be recorded")
	}
}

func TestPGApplyPulledMutation(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Apply a session mutation.
	err := s.ApplyPulledMutation("cloud", SyncMutation{
		Seq:       1,
		Entity:    SyncEntitySession,
		EntityKey: "remote-sess-1",
		Op:        SyncOpUpsert,
		Payload:   `{"id":"remote-sess-1","project":"remote-proj","directory":"/remote"}`,
	})
	if err != nil {
		t.Fatalf("ApplyPulledMutation session: %v", err)
	}

	sess, err := s.GetSession("remote-sess-1")
	if err != nil {
		t.Fatalf("GetSession after pull: %v", err)
	}
	if sess.Project != "remote-proj" {
		t.Fatalf("expected remote-proj, got %q", sess.Project)
	}
}

func TestPGSkipAckNonEnrolledMutations(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj-a", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "manual", Title: "Test", Content: "Content", Project: "proj-a",
	})

	// proj-a is NOT enrolled.
	skipped, err := s.SkipAckNonEnrolledMutations("cloud")
	if err != nil {
		t.Fatalf("SkipAckNonEnrolledMutations: %v", err)
	}
	if skipped == 0 {
		t.Fatal("expected some mutations to be skipped")
	}
}

// ─── Project Enrollment Tests ────────────────────────────────────────────────

func TestPGEnrollProjectIdempotent(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	err := s.EnrollProject("myapp")
	if err != nil {
		t.Fatalf("EnrollProject: %v", err)
	}
	// Idempotent.
	err = s.EnrollProject("myapp")
	if err != nil {
		t.Fatalf("EnrollProject idempotent: %v", err)
	}

	enrolled, _ := s.IsProjectEnrolled("myapp")
	if !enrolled {
		t.Fatal("expected myapp to be enrolled")
	}
}

func TestPGUnenrollProject(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.EnrollProject("myapp")
	err := s.UnenrollProject("myapp")
	if err != nil {
		t.Fatalf("UnenrollProject: %v", err)
	}

	enrolled, _ := s.IsProjectEnrolled("myapp")
	if enrolled {
		t.Fatal("expected myapp to be unenrolled")
	}
}

func TestPGListEnrolledProjects(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.EnrollProject("beta")
	s.EnrollProject("alpha")

	projects, err := s.ListEnrolledProjects()
	if err != nil {
		t.Fatalf("ListEnrolledProjects: %v", err)
	}
	if len(projects) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(projects))
	}
	// Should be alphabetical.
	if projects[0].Project != "alpha" || projects[1].Project != "beta" {
		t.Fatal("expected alphabetical order: alpha, beta")
	}
}

// ─── Project Migration Tests ─────────────────────────────────────────────────

func TestPGMigrateProject(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "old-name", "/dir")
	s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "manual", Title: "Test", Content: "Content", Project: "old-name",
	})
	s.AddPrompt(AddPromptParams{SessionID: "sess-1", Content: "Hello", Project: "old-name"})

	result, err := s.MigrateProject("old-name", "new-name")
	if err != nil {
		t.Fatalf("MigrateProject: %v", err)
	}
	if !result.Migrated {
		t.Fatal("expected Migrated=true")
	}
	if result.ObservationsUpdated != 1 || result.SessionsUpdated != 1 || result.PromptsUpdated != 1 {
		t.Fatalf("unexpected counts: %+v", result)
	}

	// Non-existent project is a no-op.
	result2, _ := s.MigrateProject("nonexistent", "other")
	if result2.Migrated {
		t.Fatal("expected Migrated=false for nonexistent")
	}
}

// ─── PassiveCapture Tests ────────────────────────────────────────────────────

func TestPGPassiveCapture(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	result, err := s.PassiveCapture(PassiveCaptureParams{
		SessionID: "sess-1",
		Content: `## Key Learnings:
1. Always validate input before persisting to the database to prevent corruption
2. Use structured logging for better debugging in production environments`,
		Project: "proj",
	})
	if err != nil {
		t.Fatalf("PassiveCapture: %v", err)
	}
	if result.Extracted != 2 || result.Saved != 2 {
		t.Fatalf("expected 2 extracted/saved, got %d/%d", result.Extracted, result.Saved)
	}

	// Re-capture should dedup.
	result2, _ := s.PassiveCapture(PassiveCaptureParams{
		SessionID: "sess-1",
		Content: `## Key Learnings:
1. Always validate input before persisting to the database to prevent corruption`,
		Project: "proj",
	})
	if result2.Duplicates != 1 {
		t.Fatalf("expected 1 duplicate, got %d", result2.Duplicates)
	}
}

// ─── Concurrent Access Tests ─────────────────────────────────────────────────

func TestPGConcurrentAddObservation(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")

	var wg sync.WaitGroup
	errors := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := s.AddObservation(AddObservationParams{
				SessionID: "sess-1", Type: "manual",
				Title:   fmt.Sprintf("Concurrent obs %d", n),
				Content: fmt.Sprintf("Content from goroutine %d", n),
				Project: "proj",
			})
			if err != nil {
				errors <- err
			}
		}(i)
	}
	wg.Wait()
	close(errors)

	for err := range errors {
		t.Fatalf("concurrent AddObservation failed: %v", err)
	}

	all, _ := s.AllObservations("proj", "", 100)
	if len(all) != 5 {
		t.Fatalf("expected 5 observations from concurrent writes, got %d", len(all))
	}
}

func TestPGConcurrentTopicKeyUpsert(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s.AddObservation(AddObservationParams{
				SessionID: "sess-1", Type: "decision",
				Title:    "Concurrent topic",
				Content:  fmt.Sprintf("Version %d", n),
				Project:  "proj",
				TopicKey: "concurrent/topic-test",
			})
		}(i)
	}
	wg.Wait()

	// Exactly 1 observation should exist for that topic_key.
	results, _ := s.Search("concurrent/topic-test", SearchOptions{Limit: 10})
	if len(results) != 1 {
		t.Fatalf("expected exactly 1 observation for topic_key, got %d", len(results))
	}
}

// ─── Schema Migration Idempotency Test ───────────────────────────────────────

func TestPGSchemaMigrationIdempotent(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Running migratePG again should be a no-op (already applied).
	if err := migratePG(s.pool); err != nil {
		t.Fatalf("migratePG second run: %v", err)
	}
}

// ─── GetObservationBySyncID Test ─────────────────────────────────────────────

func TestPGGetObservationBySyncID(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	s.CreateSession("sess-1", "proj", "/dir")
	id, _ := s.AddObservation(AddObservationParams{
		SessionID: "sess-1", Type: "manual", Title: "Sync test",
		Content: "Content for sync_id test", Project: "proj",
	})

	obs, _ := s.GetObservation(id)
	bySyncID, err := s.GetObservationBySyncID(obs.SyncID)
	if err != nil {
		t.Fatalf("GetObservationBySyncID: %v", err)
	}
	if bySyncID.ID != id {
		t.Fatalf("expected ID %d, got %d", id, bySyncID.ID)
	}
}

// ─── Row-Level Security Tests ────────────────────────────────────────────────

// newRLSTestEnv spins up a PG container, runs migrations, creates two non-owner
// roles (alice and bob), and returns per-user Stores whose connections execute
// SET ROLE on every checkout so current_user matches the role.
type rlsTestEnv struct {
	alice *PostgresStore
	bob   *PostgresStore
	admin *PostgresStore // owner role (engram) for direct verification
}

func newRLSTestEnv(t *testing.T) *rlsTestEnv {
	t.Helper()

	dpool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	dpool.MaxWait = 30 * time.Second

	resource, err := dpool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16-alpine",
		Env: []string{
			"POSTGRES_DB=engram_test",
			"POSTGRES_USER=engram",
			"POSTGRES_PASSWORD=test",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		t.Skipf("Could not start PG container: %v", err)
	}
	t.Cleanup(func() { dpool.Purge(resource) })

	connStr := fmt.Sprintf("postgres://engram:test@localhost:%s/engram_test?sslmode=disable",
		resource.GetPort("5432/tcp"))

	// Wait for PG to be ready and run migrations using the admin/owner pool.
	var adminPool *pgxpool.Pool
	if err := dpool.Retry(func() error {
		var err error
		adminPool, err = pgxpool.New(context.Background(), connStr)
		if err != nil {
			return err
		}
		return adminPool.Ping(context.Background())
	}); err != nil {
		t.Fatalf("pg not ready: %v", err)
	}

	if err := migratePG(adminPool); err != nil {
		t.Fatalf("pg migration: %v", err)
	}

	// Create two non-owner roles and grant them full table access.
	// They share the same login (connection via engram user) but we switch
	// with SET ROLE so current_user reflects alice/bob.
	ctx := context.Background()
	for _, ddl := range []string{
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'alice') THEN CREATE ROLE alice; END IF; END $$`,
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bob') THEN CREATE ROLE bob; END IF; END $$`,
		`GRANT USAGE ON SCHEMA public TO alice, bob`,
		`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO alice, bob`,
		`GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO alice, bob`,
		`GRANT alice TO engram`, // allow SET ROLE alice
		`GRANT bob TO engram`,   // allow SET ROLE bob
	} {
		if _, err := adminPool.Exec(ctx, ddl); err != nil {
			t.Fatalf("setup role: %v\nSQL: %s", err, ddl)
		}
	}

	cfg := Config{
		DataDir:              t.TempDir(),
		MaxObservationLength: 50000,
		MaxContextResults:    20,
		MaxSearchResults:     20,
		DedupeWindow:         time.Hour,
	}

	// Helper: create a pool that does SET ROLE <role> on every new connection.
	makeRolePool := func(role string) *pgxpool.Pool {
		pgxCfg, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			t.Fatalf("parse config for %s: %v", role, err)
		}
		pgxCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, fmt.Sprintf("SET ROLE %s", role))
			return err
		}
		p, err := pgxpool.NewWithConfig(ctx, pgxCfg)
		if err != nil {
			t.Fatalf("create pool for %s: %v", role, err)
		}
		return p
	}

	alicePool := makeRolePool("alice")
	bobPool := makeRolePool("bob")

	aliceStore := &PostgresStore{pool: alicePool, cfg: cfg, identity: "alice"}
	bobStore := &PostgresStore{pool: bobPool, cfg: cfg, identity: "bob"}
	adminStore := &PostgresStore{pool: adminPool, cfg: cfg, identity: "engram"}

	t.Cleanup(func() {
		alicePool.Close()
		bobPool.Close()
		adminPool.Close()
	})

	return &rlsTestEnv{alice: aliceStore, bob: bobStore, admin: adminStore}
}

func TestPGRLSPersonalObservationsInvisibleToOtherUsers(t *testing.T) {
	env := newRLSTestEnv(t)

	// Alice creates a session and adds a personal observation.
	if err := env.alice.CreateSession("sess-alice", "proj", "/dir"); err != nil {
		t.Fatalf("Alice CreateSession: %v", err)
	}

	alicePersonalID, err := env.alice.AddObservation(AddObservationParams{
		SessionID: "sess-alice", Type: "decision",
		Title: "Alice private note", Content: "Secret stuff only Alice should see",
		Project: "proj", Scope: "personal",
	})
	if err != nil {
		t.Fatalf("Alice AddObservation (personal): %v", err)
	}
	if alicePersonalID == 0 {
		t.Fatal("expected non-zero observation ID for Alice's personal observation")
	}

	// Alice also adds a project observation (should be visible to all).
	aliceProjectID, err := env.alice.AddObservation(AddObservationParams{
		SessionID: "sess-alice", Type: "decision",
		Title: "Team decision", Content: "We use PostgreSQL for storage",
		Project: "proj", Scope: "project",
	})
	if err != nil {
		t.Fatalf("Alice AddObservation (project): %v", err)
	}

	// Bob creates his own session (needed for query operations).
	if err := env.bob.CreateSession("sess-bob", "proj", "/dir"); err != nil {
		t.Fatalf("Bob CreateSession: %v", err)
	}

	// ── Verify: Bob queries all observations ──
	bobAll, err := env.bob.AllObservations("proj", "", 100)
	if err != nil {
		t.Fatalf("Bob AllObservations: %v", err)
	}

	var bobSeesPersonal, bobSeesProject bool
	for _, obs := range bobAll {
		if obs.ID == alicePersonalID {
			bobSeesPersonal = true
		}
		if obs.ID == aliceProjectID {
			bobSeesProject = true
		}
	}

	if bobSeesPersonal {
		t.Error("SECURITY VIOLATION: Bob can see Alice's personal observation")
	}
	if !bobSeesProject {
		t.Error("Bob should see Alice's project observation but doesn't")
	}

	// ── Verify: Alice CAN see her own personal observation ──
	aliceAll, err := env.alice.AllObservations("proj", "", 100)
	if err != nil {
		t.Fatalf("Alice AllObservations: %v", err)
	}

	var aliceSeesOwn bool
	for _, obs := range aliceAll {
		if obs.ID == alicePersonalID {
			aliceSeesOwn = true
		}
	}
	if !aliceSeesOwn {
		t.Error("Alice should see her own personal observation but doesn't")
	}

	// ── Verify: Search also respects RLS ──
	bobSearch, err := env.bob.Search("secret", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Bob Search: %v", err)
	}
	for _, r := range bobSearch {
		if r.ID == alicePersonalID {
			t.Error("SECURITY VIOLATION: Bob's search returned Alice's personal observation")
		}
	}

	// Alice's search should find her personal observation.
	aliceSearch, err := env.alice.Search("secret", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Alice Search: %v", err)
	}
	var aliceSearchFinds bool
	for _, r := range aliceSearch {
		if r.ID == alicePersonalID {
			aliceSearchFinds = true
		}
	}
	if !aliceSearchFinds {
		t.Error("Alice's search should find her own personal observation")
	}
}

func TestPGRLSPromptsInvisibleToOtherUsers(t *testing.T) {
	env := newRLSTestEnv(t)

	// Alice creates a session and adds a prompt.
	if err := env.alice.CreateSession("sess-alice", "proj", "/dir"); err != nil {
		t.Fatalf("Alice CreateSession: %v", err)
	}

	alicePromptID, err := env.alice.AddPrompt(AddPromptParams{
		SessionID: "sess-alice", Content: "How do I fix the auth middleware?",
		Project: "proj",
	})
	if err != nil {
		t.Fatalf("Alice AddPrompt: %v", err)
	}

	// Bob creates his own session.
	if err := env.bob.CreateSession("sess-bob", "proj", "/dir"); err != nil {
		t.Fatalf("Bob CreateSession: %v", err)
	}

	// Bob adds his own prompt.
	_, err = env.bob.AddPrompt(AddPromptParams{
		SessionID: "sess-bob", Content: "What's the deployment pipeline?",
		Project: "proj",
	})
	if err != nil {
		t.Fatalf("Bob AddPrompt: %v", err)
	}

	// ── Verify: Bob's recent prompts don't include Alice's ──
	bobPrompts, err := env.bob.RecentPrompts("proj", 100)
	if err != nil {
		t.Fatalf("Bob RecentPrompts: %v", err)
	}
	for _, p := range bobPrompts {
		if p.ID == alicePromptID {
			t.Error("SECURITY VIOLATION: Bob can see Alice's prompt")
		}
	}

	// ── Verify: Alice can see her own ──
	alicePrompts, err := env.alice.RecentPrompts("proj", 100)
	if err != nil {
		t.Fatalf("Alice RecentPrompts: %v", err)
	}
	var aliceSeesOwn bool
	for _, p := range alicePrompts {
		if p.ID == alicePromptID {
			aliceSeesOwn = true
		}
	}
	if !aliceSeesOwn {
		t.Error("Alice should see her own prompt")
	}
}

func TestPGRLSPersonalUpdateDeleteBlocked(t *testing.T) {
	env := newRLSTestEnv(t)

	// Alice creates a session and a personal observation.
	if err := env.alice.CreateSession("sess-alice", "proj", "/dir"); err != nil {
		t.Fatalf("Alice CreateSession: %v", err)
	}

	aliceObsID, err := env.alice.AddObservation(AddObservationParams{
		SessionID: "sess-alice", Type: "decision",
		Title: "Alice personal", Content: "My secret note",
		Project: "proj", Scope: "personal",
	})
	if err != nil {
		t.Fatalf("Alice AddObservation: %v", err)
	}

	// Bob tries to update Alice's personal observation — should silently fail
	// (RLS filters out the row so the UPDATE WHERE matches 0 rows).
	newContent := "Hacked by Bob!"
	if _, err := env.bob.UpdateObservation(aliceObsID, UpdateObservationParams{
		Content: &newContent,
	}); err != nil {
		// Not necessarily an error — the update just won't match any rows.
		t.Logf("Bob UpdateObservation returned: %v (expected — RLS blocks it)", err)
	}

	// Verify the content is unchanged (Alice checks her own observation).
	obs, err := env.alice.GetObservation(aliceObsID)
	if err != nil {
		t.Fatalf("Alice GetObservation: %v", err)
	}
	if obs.Content == "Hacked by Bob!" {
		t.Error("SECURITY VIOLATION: Bob was able to update Alice's personal observation")
	}
	if obs.Content != "My secret note" {
		t.Errorf("unexpected content after Bob's update attempt: %q", obs.Content)
	}

	// Bob tries to soft-delete — should also be blocked.
	if err := env.bob.DeleteObservation(aliceObsID, false); err != nil {
		t.Logf("Bob DeleteObservation returned: %v (expected — RLS blocks it)", err)
	}

	// Verify it's still alive.
	obs2, err := env.alice.GetObservation(aliceObsID)
	if err != nil {
		t.Fatalf("Alice GetObservation after Bob's delete attempt: %v", err)
	}
	if obs2.DeletedAt != nil {
		t.Error("SECURITY VIOLATION: Bob was able to soft-delete Alice's personal observation")
	}
}

func TestPGRLSProjectScopeVisibleToAll(t *testing.T) {
	env := newRLSTestEnv(t)

	// Alice creates project-scope data.
	if err := env.alice.CreateSession("sess-alice", "proj", "/dir"); err != nil {
		t.Fatalf("Alice CreateSession: %v", err)
	}

	_, err := env.alice.AddObservation(AddObservationParams{
		SessionID: "sess-alice", Type: "architecture",
		Title: "Shared architecture decision", Content: "We use hexagonal architecture",
		Project: "proj", Scope: "project",
	})
	if err != nil {
		t.Fatalf("Alice AddObservation (project): %v", err)
	}

	// Bob can see it in search.
	if err := env.bob.CreateSession("sess-bob", "proj", "/dir"); err != nil {
		t.Fatalf("Bob CreateSession: %v", err)
	}

	results, err := env.bob.Search("hexagonal", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Bob Search: %v", err)
	}
	if len(results) == 0 {
		t.Error("Bob should see Alice's project-scope observation in search")
	}

	// Bob can also see it in timeline and stats.
	bobAll, err := env.bob.AllObservations("proj", "", 100)
	if err != nil {
		t.Fatalf("Bob AllObservations: %v", err)
	}
	if len(bobAll) == 0 {
		t.Error("Bob should see project-scope observations in AllObservations")
	}
}

func TestPGRLSMigrationIdempotent(t *testing.T) {
	env := newRLSTestEnv(t)

	// Running migrations again should not fail (DROP POLICY IF EXISTS).
	if err := migratePG(env.admin.pool); err != nil {
		t.Fatalf("second migratePG run failed: %v", err)
	}
}

// ─── Relations — PostgresStore ────────────────────────────────────────────────

func TestPGSaveAndGetRelation(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	syncID := newSyncID("rel")
	rel, err := s.SaveRelation(SaveRelationParams{
		SyncID:   syncID,
		SourceID: "obs-src-001",
		TargetID: "obs-tgt-001",
	})
	if err != nil {
		t.Fatalf("SaveRelation: %v", err)
	}
	if rel.SyncID != syncID {
		t.Errorf("expected sync_id %q, got %q", syncID, rel.SyncID)
	}
	if rel.Relation != "pending" {
		t.Errorf("expected initial relation 'pending', got %q", rel.Relation)
	}
	if rel.JudgmentStatus != "pending" {
		t.Errorf("expected initial judgment_status 'pending', got %q", rel.JudgmentStatus)
	}

	// Verify GetRelation round-trip.
	got, err := s.GetRelation(syncID)
	if err != nil {
		t.Fatalf("GetRelation: %v", err)
	}
	if got.SyncID != syncID {
		t.Errorf("GetRelation SyncID mismatch: %q vs %q", got.SyncID, syncID)
	}
	if got.CreatedAt == "" {
		t.Error("GetRelation: CreatedAt is empty (PG timestamp scan failed)")
	}
}

func TestPGGetRelation_NotFound(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	_, err := s.GetRelation("rel-nonexistent-000")
	if err == nil {
		t.Fatal("expected error for missing relation, got nil")
	}
}

func TestPGJudgeRelation(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	syncID := newSyncID("rel")
	if _, err := s.SaveRelation(SaveRelationParams{
		SyncID:   syncID,
		SourceID: "obs-src-002",
		TargetID: "obs-tgt-002",
	}); err != nil {
		t.Fatalf("SaveRelation: %v", err)
	}

	reason := "they share the same topic"
	rel, err := s.JudgeRelation(JudgeRelationParams{
		JudgmentID:    syncID,
		Relation:      RelationRelated,
		Reason:        &reason,
		MarkedByActor: "test-agent",
		MarkedByKind:  "agent",
	})
	if err != nil {
		t.Fatalf("JudgeRelation: %v", err)
	}
	if rel.Relation != RelationRelated {
		t.Errorf("expected relation %q, got %q", RelationRelated, rel.Relation)
	}
	if rel.JudgmentStatus != "judged" {
		t.Errorf("expected status 'judged', got %q", rel.JudgmentStatus)
	}
	if rel.Reason == nil || *rel.Reason != reason {
		t.Errorf("expected reason %q, got %v", reason, rel.Reason)
	}
}

func TestPGJudgeRelation_InvalidVerb(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	syncID := newSyncID("rel")
	if _, err := s.SaveRelation(SaveRelationParams{
		SyncID:   syncID,
		SourceID: "obs-src-003",
		TargetID: "obs-tgt-003",
	}); err != nil {
		t.Fatalf("SaveRelation: %v", err)
	}

	_, err := s.JudgeRelation(JudgeRelationParams{
		JudgmentID:    syncID,
		Relation:      "invalid_verb",
		MarkedByActor: "test",
		MarkedByKind:  "agent",
	})
	if err == nil {
		t.Fatal("expected error for invalid verb, got nil")
	}
}

func TestPGJudgeBySemantic_InsertAndUpdate(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Insert path.
	sid, err := s.JudgeBySemantic(JudgeBySemanticParams{
		SourceID:   "obs-src-004",
		TargetID:   "obs-tgt-004",
		Relation:   RelationCompatible,
		Confidence: 0.85,
		Reasoning:  "compatible topics",
		Model:      "claude-sonnet-4-6",
	})
	if err != nil {
		t.Fatalf("JudgeBySemantic insert: %v", err)
	}
	if sid == "" {
		t.Fatal("expected non-empty sync_id")
	}

	// Idempotent update path (same pair, different verb).
	sid2, err := s.JudgeBySemantic(JudgeBySemanticParams{
		SourceID:   "obs-src-004",
		TargetID:   "obs-tgt-004",
		Relation:   RelationRelated,
		Confidence: 0.90,
		Reasoning:  "updated judgment",
		Model:      "claude-sonnet-4-6",
	})
	if err != nil {
		t.Fatalf("JudgeBySemantic update: %v", err)
	}
	if sid2 != sid {
		t.Errorf("expected same sync_id on upsert: %q vs %q", sid2, sid)
	}

	// Verify via GetRelation.
	rel, err := s.GetRelation(sid)
	if err != nil {
		t.Fatalf("GetRelation after JudgeBySemantic: %v", err)
	}
	if rel.Relation != RelationRelated {
		t.Errorf("expected updated relation %q, got %q", RelationRelated, rel.Relation)
	}
}

func TestPGJudgeBySemantic_NotConflictIsNoop(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	sid, err := s.JudgeBySemantic(JudgeBySemanticParams{
		SourceID:   "obs-src-005",
		TargetID:   "obs-tgt-005",
		Relation:   RelationNotConflict,
		Confidence: 0.99,
	})
	if err != nil {
		t.Fatalf("JudgeBySemantic not_conflict: %v", err)
	}
	if sid != "" {
		t.Errorf("expected empty sync_id for not_conflict noop, got %q", sid)
	}
}

func TestPGListAndCountRelations(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Insert a few relations.
	for i := 0; i < 3; i++ {
		if _, err := s.SaveRelation(SaveRelationParams{
			SyncID:   newSyncID("rel"),
			SourceID: fmt.Sprintf("obs-list-src-%d", i),
			TargetID: fmt.Sprintf("obs-list-tgt-%d", i),
		}); err != nil {
			t.Fatalf("SaveRelation %d: %v", i, err)
		}
	}

	items, err := s.ListRelations(ListRelationsOptions{})
	if err != nil {
		t.Fatalf("ListRelations: %v", err)
	}
	if len(items) < 3 {
		t.Errorf("expected at least 3 items, got %d", len(items))
	}
	for _, item := range items {
		if item.CreatedAt == "" {
			t.Error("ListRelations: item.CreatedAt is empty (PG timestamp scan failed)")
		}
	}

	total, err := s.CountRelations(ListRelationsOptions{})
	if err != nil {
		t.Fatalf("CountRelations: %v", err)
	}
	if total < 3 {
		t.Errorf("expected total >= 3, got %d", total)
	}

	// Filter by status.
	pending, err := s.CountRelations(ListRelationsOptions{Status: "pending"})
	if err != nil {
		t.Fatalf("CountRelations pending: %v", err)
	}
	if pending < 3 {
		t.Errorf("expected at least 3 pending, got %d", pending)
	}
}

func TestPGGetRelationStats(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Insert a relation and judge it so we have both pending and judged rows.
	sid := newSyncID("rel")
	if _, err := s.SaveRelation(SaveRelationParams{
		SyncID:   sid,
		SourceID: "obs-stats-src-1",
		TargetID: "obs-stats-tgt-1",
	}); err != nil {
		t.Fatalf("SaveRelation: %v", err)
	}

	stats, err := s.GetRelationStats("")
	if err != nil {
		t.Fatalf("GetRelationStats: %v", err)
	}
	if stats.ByJudgmentStatus["pending"] < 1 {
		t.Errorf("expected at least 1 pending in stats, got %v", stats.ByJudgmentStatus)
	}
}

func TestPGCountDeferredAndDead(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// Empty table — should return 0,0 without error.
	deferred, dead, err := s.CountDeferredAndDead()
	if err != nil {
		t.Fatalf("CountDeferredAndDead: %v", err)
	}
	if deferred != 0 || dead != 0 {
		t.Errorf("expected 0,0 on empty table, got %d,%d", deferred, dead)
	}
}

func TestPGGetRelationsForObservations(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	srcID := "obs-rel-src-007"
	tgtID := "obs-rel-tgt-007"
	sid := newSyncID("rel")
	if _, err := s.SaveRelation(SaveRelationParams{
		SyncID:   sid,
		SourceID: srcID,
		TargetID: tgtID,
	}); err != nil {
		t.Fatalf("SaveRelation: %v", err)
	}

	result, err := s.GetRelationsForObservations([]string{srcID, tgtID})
	if err != nil {
		t.Fatalf("GetRelationsForObservations: %v", err)
	}

	srcRels, ok := result[srcID]
	if !ok {
		t.Fatalf("expected entry for srcID in result map")
	}
	if len(srcRels.AsSource) == 0 {
		t.Errorf("expected at least one AsSource relation for srcID")
	}
	if srcRels.AsSource[0].SyncID != sid {
		t.Errorf("expected SyncID %q, got %q", sid, srcRels.AsSource[0].SyncID)
	}
	if srcRels.AsSource[0].CreatedAt == "" {
		t.Error("GetRelationsForObservations: CreatedAt is empty (PG timestamp scan failed)")
	}

	tgtRels := result[tgtID]
	if len(tgtRels.AsTarget) == 0 {
		t.Errorf("expected at least one AsTarget relation for tgtID")
	}

	// Empty input — should return empty map without error.
	empty, err := s.GetRelationsForObservations([]string{})
	if err != nil {
		t.Fatalf("GetRelationsForObservations empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected empty map, got %d entries", len(empty))
	}
}

func TestPGFindCandidates_SkipInsert(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	// PG enforces a FK on session_id — create sessions first.
	if err := s.CreateSession("sess-fc-001", "test-project", "/tmp"); err != nil {
		t.Fatalf("CreateSession 1: %v", err)
	}
	if err := s.CreateSession("sess-fc-002", "test-project", "/tmp"); err != nil {
		t.Fatalf("CreateSession 2: %v", err)
	}

	// Add a source observation.
	srcID, err := s.AddObservation(AddObservationParams{
		SessionID: "sess-fc-001",
		Type:      "manual",
		Title:     "JWT authentication token refresh flow",
		Content:   "How JWT tokens are refreshed using the refresh token endpoint",
		Project:   "test-project",
		Scope:     "project",
	})
	if err != nil {
		t.Fatalf("AddObservation source: %v", err)
	}

	// Add a candidate observation with similar content.
	_, err = s.AddObservation(AddObservationParams{
		SessionID: "sess-fc-002",
		Type:      "manual",
		Title:     "JWT token expiration handling",
		Content:   "Handling expired JWT tokens and refreshing them",
		Project:   "test-project",
		Scope:     "project",
	})
	if err != nil {
		t.Fatalf("AddObservation candidate: %v", err)
	}

	// FindCandidates with SkipInsert=true: no relation rows created.
	candidates, err := s.FindCandidates(srcID, CandidateOptions{
		Project:    "test-project",
		Scope:      "project",
		Limit:      5,
		SkipInsert: true,
	})
	if err != nil {
		t.Fatalf("FindCandidates SkipInsert: %v", err)
	}
	// Result may be empty if FTS index hasn't updated yet, but must not error.
	for _, c := range candidates {
		if c.JudgmentID != "" {
			t.Error("SkipInsert=true should not produce JudgmentID")
		}
	}
	t.Logf("FindCandidates SkipInsert returned %d candidates", len(candidates))
}

// ─── ListDeferred / GetDeferred (PostgreSQL) ─────────────────────────────────

func seedDeferredRowPG(t *testing.T, s *PostgresStore, syncID, entity, payload string, retryCount int, applyStatus string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO sync_apply_deferred
			(sync_id, entity, payload, apply_status, retry_count, first_seen_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, syncID, entity, payload, applyStatus, retryCount); err != nil {
		t.Fatalf("seedDeferredRowPG %q: %v", syncID, err)
	}
}

func TestPGListDeferred_HappyPath(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	validPayload := `{"relation_type":"conflicts_with","source_id":"obs-pg1","target_id":"obs-pg2"}`
	seedDeferredRowPG(t, s, "pg-def-001", "relation", validPayload, 0, "deferred")
	seedDeferredRowPG(t, s, "pg-def-002", "relation", validPayload, 1, "deferred")
	seedDeferredRowPG(t, s, "pg-def-003", "relation", validPayload, 5, "dead")

	// List all.
	all, err := s.ListDeferred(ListDeferredOptions{Limit: 50})
	if err != nil {
		t.Fatalf("ListDeferred all: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 rows; got %d", len(all))
	}

	// List only deferred status.
	deferred, err := s.ListDeferred(ListDeferredOptions{Status: "deferred", Limit: 50})
	if err != nil {
		t.Fatalf("ListDeferred deferred: %v", err)
	}
	if len(deferred) != 2 {
		t.Errorf("expected 2 deferred rows; got %d", len(deferred))
	}

	// Pagination: limit=1.
	page, err := s.ListDeferred(ListDeferredOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListDeferred limit=1: %v", err)
	}
	if len(page) != 1 {
		t.Errorf("expected 1 row with limit=1; got %d", len(page))
	}
}

func TestPGListDeferred_DecodedPayload(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	validPayload := `{"relation_type":"related","source_id":"obs-src","target_id":"obs-tgt","extra":99}`
	seedDeferredRowPG(t, s, "pg-def-valid", "relation", validPayload, 0, "deferred")

	rows, err := s.ListDeferred(ListDeferredOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeferred: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row; got %d", len(rows))
	}
	row := rows[0]
	if !row.PayloadValid {
		t.Errorf("expected PayloadValid=true; got false. PayloadRaw=%q", row.PayloadRaw)
	}
	if row.Payload["relation_type"] != "related" {
		t.Errorf("Payload[relation_type]: want related; got %v", row.Payload["relation_type"])
	}
}

func TestPGListDeferred_MalformedPayload(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	seedDeferredRowPG(t, s, "pg-def-bad", "relation", "not valid json", 3, "dead")

	rows, err := s.ListDeferred(ListDeferredOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeferred malformed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row; got %d", len(rows))
	}
	row := rows[0]
	if row.PayloadValid {
		t.Errorf("expected PayloadValid=false for malformed JSON; got true")
	}
	if row.PayloadRaw != "not valid json" {
		t.Errorf("expected PayloadRaw preserved; got %q", row.PayloadRaw)
	}
}

func TestPGGetDeferred_HappyPath(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	validPayload := `{"relation_type":"compatible","source_id":"obs-abc","target_id":"obs-def"}`
	seedDeferredRowPG(t, s, "pg-def-xyz", "relation", validPayload, 2, "deferred")

	row, err := s.GetDeferred("pg-def-xyz")
	if err != nil {
		t.Fatalf("GetDeferred: %v", err)
	}
	if row.SyncID != "pg-def-xyz" {
		t.Errorf("expected SyncID=pg-def-xyz; got %q", row.SyncID)
	}
	if row.ApplyStatus != "deferred" {
		t.Errorf("expected ApplyStatus=deferred; got %q", row.ApplyStatus)
	}
	if row.RetryCount != 2 {
		t.Errorf("expected RetryCount=2; got %d", row.RetryCount)
	}
	if !row.PayloadValid {
		t.Errorf("expected PayloadValid=true; got false")
	}
	if row.Payload["relation_type"] != "compatible" {
		t.Errorf("Payload[relation_type]: want compatible; got %v", row.Payload["relation_type"])
	}
}

func TestPGGetDeferred_NotFound(t *testing.T) {
	s := newTestStorePG(t)
	defer s.Close()

	_, err := s.GetDeferred("pg-def-missing")
	if err == nil {
		t.Fatal("expected error for missing sync_id; got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected error to contain 'not found'; got %q", err.Error())
	}
}

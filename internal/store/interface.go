package store

import (
	"context"
	"database/sql"
	"time"
)

// Store is the persistence contract satisfied by every Engram backend. The
// SQLite-backed *SQLiteStore and the PostgreSQL-backed *PostgresStore both
// implement this interface, letting the rest of the codebase depend on
// behavior instead of a concrete struct.
type Store interface {
	// Lifecycle
	Close() error
	Identity() string
	MaxObservationLength() int

	// Sessions
	CreateSession(id, project, directory string) error
	EndSession(id string, summary string) error
	GetSession(id string) (*Session, error)
	RecentSessions(project string, limit int) ([]SessionSummary, error)
	AllSessions(project string, limit int) ([]SessionSummary, error)
	DeleteSession(id string) error

	// Observations
	AddObservation(p AddObservationParams) (int64, error)
	GetObservation(id int64) (*Observation, error)
	GetObservationBySyncID(syncID string) (*Observation, error)
	UpdateObservation(id int64, p UpdateObservationParams) (*Observation, error)
	DeleteObservation(id int64, hardDelete bool) error
	AllObservations(project, scope string, limit int) ([]Observation, error)
	SessionObservations(sessionID string, limit int) ([]Observation, error)
	RecentObservations(project, scope string, limit int) ([]Observation, error)

	// Prompts
	AddPrompt(p AddPromptParams) (int64, error)
	RecentPrompts(project string, limit int) ([]Prompt, error)
	SearchPrompts(query string, project string, limit int) ([]Prompt, error)
	DeletePrompt(id int64) error

	// Search / context / stats
	Search(query string, opts SearchOptions) ([]SearchResult, error)
	Timeline(observationID int64, before, after int) (*TimelineResult, error)
	Stats() (*Stats, error)
	FormatContext(project, scope string) (string, error)

	// Export / Import
	Export() (*ExportData, error)
	Import(data *ExportData) (*ImportResult, error)

	// Sync — chunk-level
	GetSyncedChunks() (map[string]bool, error)
	RecordSyncedChunk(chunkID string) error

	// Sync — mutation journal
	GetSyncState(targetKey string) (*SyncState, error)
	ListPendingSyncMutations(targetKey string, limit int) ([]SyncMutation, error)
	CountPendingNonEnrolledSyncMutations(targetKey string) ([]PendingSyncMutationProjectCount, error)
	SkipAckNonEnrolledMutations(targetKey string) (int64, error)
	AckSyncMutations(targetKey string, lastAckedSeq int64) error
	AckSyncMutationSeqs(targetKey string, seqs []int64) error
	AcquireSyncLease(targetKey, owner string, ttl time.Duration, now time.Time) (bool, error)
	ReleaseSyncLease(targetKey, owner string) error
	MarkSyncFailure(targetKey, message string, backoffUntil time.Time) error
	MarkSyncBlocked(targetKey, reasonCode, message string) error
	MarkSyncHealthy(targetKey string) error
	ApplyPulledMutation(targetKey string, mutation SyncMutation) error
	ReplayDeferred() (ReplayDeferredResult, error)

	// Project enrollment
	EnrollProject(project string) error
	UnenrollProject(project string) error
	ListEnrolledProjects() ([]EnrolledProject, error)
	IsProjectEnrolled(project string) (bool, error)

	// Project lifecycle / introspection
	ProjectExists(name string) (bool, error)
	MigrateProject(oldName, newName string) (*MigrateResult, error)
	ListProjectNames() ([]string, error)
	ListProjects(includeDeprecated bool) ([]ProjectStats, error)
	ListProjectsWithStats() ([]ProjectDetailStats, error)
	CountObservationsForProject(name string) (int, error)
	MergeProjects(sources []string, canonical string) (*MergeResult, error)
	PruneProject(project string) (*PruneResult, error)
	DeprecateProject(project, identity string) error
	ActivateProject(project string) error
	IsProjectDeprecated(project string) (bool, error)

	// Promotion / contributors / passive capture
	PromoteObservation(id int64, identity string) error
	ListContributors(project string) ([]ContributorStats, error)
	PassiveCapture(p PassiveCaptureParams) (*PassiveCaptureResult, error)

	// Memory relations — conflict surfacing (mem_judge / mem_compare subsystem)
	FindCandidates(savedID int64, opts CandidateOptions) ([]Candidate, error)
	SaveRelation(p SaveRelationParams) (*Relation, error)
	GetRelation(syncID string) (*Relation, error)
	JudgeRelation(p JudgeRelationParams) (*Relation, error)
	JudgeBySemantic(p JudgeBySemanticParams) (string, error)
	GetRelationsForObservations(syncIDs []string) (map[string]ObservationRelations, error)
	ListRelations(opts ListRelationsOptions) ([]RelationListItem, error)
	CountRelations(opts ListRelationsOptions) (int, error)
	GetRelationStats(project string) (RelationStats, error)
	CountDeferredAndDead() (deferred, dead int, err error)

	// Deferred queue — read-only access to sync_apply_deferred rows.
	ListDeferred(opts ListDeferredOptions) ([]DeferredRow, error)
	GetDeferred(syncID string) (DeferredRow, error)

	// Semantic scan — walks a project's observations and optionally LLM-judges pairs.
	ScanProject(opts ScanOptions) (ScanResult, error)

	// Operational diagnostics (doctor subsystem)
	ListDiagnosticSessions(project string) ([]DiagnosticSessionEvidence, error)
	ListPendingProjectMutations(project string) ([]SyncMutation, error)
	ReadSQLiteLockSnapshot(ctx context.Context) (SQLiteLockSnapshot, error)
	EstimateSessionProjectReclassification(actions []SessionProjectReclassification) (SessionProjectReclassificationCounts, error)
	ApplySessionProjectReclassification(actions []SessionProjectReclassification) (SessionProjectReclassificationResult, error)
	BackupSQLite() (string, error)

	// ─── v1.20.0 parity ──────────────────────────────────────────────────────
	PinnedObservations(project, scope string) ([]Observation, error)
	ListObservationSyncPayloads() ([]any, error)
	CountRelationSyncMutations() (int, error)
	// DB exposes the underlying *sql.DB for test helpers and integration tests.
	// PostgreSQL-backed stores return nil — they are not database/sql based.
	DB() *sql.DB
	ClearCloudUpgradeState(project string) error
	DeleteProject(project string, hardDelete bool) (*DeleteProjectResult, error)
	MarkSyncAuthRequired(targetKey, message string) error
	HasPendingSyncMutationsForProject(project string) (bool, error)
	MarkSyncPending(targetKey string) error
	DiagnoseCloudUpgradeLegacyMutations(project string) (CloudUpgradeLegacyMutationReport, error)
	RepairCloudUpgrade(project string, apply bool) (CloudUpgradeRepairReport, error)
	SaveCloudUpgradeState(state CloudUpgradeState) error
	GetRelationByIntID(id int64) (*RelationListItem, error)
	AddPromptIfMissing(p AddPromptParams) (int64, bool, error)
	ApplyPulledChunk(targetKey, chunkID string, mutations []SyncMutation) error
	CanRollbackCloudUpgrade(project string) (bool, error)
	ExportProject(project string) (*ExportData, error)
	ExportRelationMutations(project string) ([]SyncMutation, error)
	GetCloudUpgradeState(project string) (*CloudUpgradeState, error)
	GetSyncedChunksForTarget(targetKey string) (map[string]bool, error)
	ListPendingSyncMutationsAfterSeq(targetKey string, afterSeq int64, limit int) ([]SyncMutation, error)
	MarkReviewed(id int64) error
	MostRecentActiveSession(project string) (string, bool, error)
	ObservationsNeedingReview(project string, limit int) ([]Observation, error)
	PinObservation(id int64) error
	RecordSyncedChunkForTarget(targetKey, chunkID string) error
	RollbackCloudUpgrade(project string) (CloudUpgradeState, error)
	UnpinObservation(id int64) error
}

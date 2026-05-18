package autosync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Gentleman-Programming/engram/internal/store"
)

// ─── Fakes ───────────────────────────────────────────────────────────────────

type fakeLocalStore struct {
	mu                sync.Mutex
	mutations         []store.SyncMutation
	syncState         *store.SyncState
	leaseOwner        string
	pushErr           error
	pullErr           error
	failureMessage    string
	blockedReason     string
	blockedMessage    string
	appliedMuts       []store.SyncMutation
	acquireGranted    bool
	ackedSeqs         []int64
	nonEnrolledCounts []store.PendingSyncMutationProjectCount
}

func newFakeLocalStore() *fakeLocalStore {
	return &fakeLocalStore{
		acquireGranted: true,
		syncState: &store.SyncState{
			TargetKey:     "cloud",
			Lifecycle:     "idle",
			LastPulledSeq: 0,
		},
	}
}

func (s *fakeLocalStore) GetSyncState(_ string) (*store.SyncState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pullErr != nil {
		return nil, s.pullErr
	}
	return s.syncState, nil
}

func (s *fakeLocalStore) ListPendingSyncMutations(_ string, limit int) ([]store.SyncMutation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pushErr != nil {
		return nil, s.pushErr
	}
	if len(s.mutations) == 0 {
		return nil, nil
	}
	n := len(s.mutations)
	if limit > 0 && n > limit {
		n = limit
	}
	return s.mutations[:n], nil
}

func (s *fakeLocalStore) CountPendingNonEnrolledSyncMutations(_ string) ([]store.PendingSyncMutationProjectCount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.PendingSyncMutationProjectCount(nil), s.nonEnrolledCounts...), nil
}

func (s *fakeLocalStore) AckSyncMutations(_ string, _ int64) error { return nil }

func (s *fakeLocalStore) AckSyncMutationSeqs(_ string, seqs []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackedSeqs = append(s.ackedSeqs, seqs...)
	return nil
}

func (s *fakeLocalStore) AcquireSyncLease(_, owner string, _ time.Duration, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.acquireGranted {
		return false, nil
	}
	s.leaseOwner = owner
	return true, nil
}

func (s *fakeLocalStore) ReleaseSyncLease(_, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaseOwner = ""
	return nil
}

func (s *fakeLocalStore) ApplyPulledMutation(_ string, mutation store.SyncMutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pullErr != nil {
		return s.pullErr
	}
	s.appliedMuts = append(s.appliedMuts, mutation)
	return nil
}

func (s *fakeLocalStore) MarkSyncFailure(_, message string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failureMessage = message
	return nil
}

func (s *fakeLocalStore) MarkSyncBlocked(_, reasonCode, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockedReason = reasonCode
	s.blockedMessage = message
	return nil
}

func (s *fakeLocalStore) MarkSyncHealthy(_ string) error { return nil }

func (s *fakeLocalStore) ReplayDeferred() (store.ReplayDeferredResult, error) {
	return store.ReplayDeferredResult{}, nil
}

func (s *fakeLocalStore) CountDeferredAndDead() (int, int, error) { return 0, 0, nil }

// ─── Fake Transport ───────────────────────────────────────────────────────────

type fakeCloudTransport struct {
	mu         sync.Mutex
	pushErr    error
	pullErr    error
	pushCalls  int32
	pullCalls  int32
	pushResult *PushMutationsResult
	pullResult *PullMutationsResponse
	pushed     [][]MutationEntry
}

type fakeRepairableCloudError struct{ msg string }

func (e fakeRepairableCloudError) Error() string       { return e.msg }
func (e fakeRepairableCloudError) IsRepairable() bool  { return true }

func newFakeTransport() *fakeCloudTransport {
	return &fakeCloudTransport{
		pushResult: &PushMutationsResult{AcceptedSeqs: []int64{}},
		pullResult: &PullMutationsResponse{Mutations: []PulledMutation{}},
	}
}

func (t *fakeCloudTransport) PushMutations(mutations []MutationEntry) (*PushMutationsResult, error) {
	atomic.AddInt32(&t.pushCalls, 1)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pushErr != nil {
		return nil, t.pushErr
	}
	batch := append([]MutationEntry(nil), mutations...)
	t.pushed = append(t.pushed, batch)
	return t.pushResult, nil
}

func (t *fakeCloudTransport) PullMutations(_ int64, _ int) (*PullMutationsResponse, error) {
	atomic.AddInt32(&t.pullCalls, 1)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pullErr != nil {
		return nil, t.pullErr
	}
	return t.pullResult, nil
}

// ─── Push ack safety tests ────────────────────────────────────────────────────

func TestManagerPushNoPendingDoesNotPushOrAck(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	mgr := New(ls, tr, DefaultConfig())

	if err := mgr.push(context.Background()); err != nil {
		t.Fatalf("push: %v", err)
	}

	if got := atomic.LoadInt32(&tr.pushCalls); got != 0 {
		t.Fatalf("expected no transport push without pending mutations, got %d calls", got)
	}
	ls.mu.Lock()
	acked := append([]int64(nil), ls.ackedSeqs...)
	ls.mu.Unlock()
	if len(acked) != 0 {
		t.Fatalf("expected no ack without pending mutations, got %v", acked)
	}
}

func TestManagerPushAcksPendingMutationsAfterTransportSuccess(t *testing.T) {
	ls := newFakeLocalStore()
	ls.mutations = []store.SyncMutation{
		{Seq: 1, Entity: "obs", EntityKey: "k1", Op: "upsert", Project: "proj-a", Payload: `{"id":"1"}`},
		{Seq: 2, Entity: "obs", EntityKey: "k2", Op: "upsert", Project: "proj-a", Payload: `{"id":"2"}`},
	}
	tr := newFakeTransport()
	tr.pushResult = &PushMutationsResult{AcceptedSeqs: []int64{101, 102}}
	mgr := New(ls, tr, DefaultConfig())

	if err := mgr.push(context.Background()); err != nil {
		t.Fatalf("push: %v", err)
	}

	if got := atomic.LoadInt32(&tr.pushCalls); got != 1 {
		t.Fatalf("expected one transport push, got %d", got)
	}
	ls.mu.Lock()
	acked := append([]int64(nil), ls.ackedSeqs...)
	ls.mu.Unlock()
	if fmt.Sprint(acked) != "[1 2]" {
		t.Fatalf("expected original local seqs [1 2] after successful push, got %v", acked)
	}
}

func TestManagerPushDoesNotAckWhenAcceptedSeqCountMismatchesBatch(t *testing.T) {
	tests := []struct {
		name         string
		pushResult   *PushMutationsResult
		wantErrPiece string
	}{
		{"nil result", nil, "missing accepted seqs"},
		{"no accepted seqs", &PushMutationsResult{AcceptedSeqs: []int64{}}, "accepted 0 of 2"},
		{"short accepted seqs", &PushMutationsResult{AcceptedSeqs: []int64{101}}, "accepted 1 of 2"},
		{"long accepted seqs", &PushMutationsResult{AcceptedSeqs: []int64{101, 102, 103}}, "accepted 3 of 2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := newFakeLocalStore()
			ls.mutations = []store.SyncMutation{
				{Seq: 1, Entity: "obs", EntityKey: "k1", Op: "upsert", Project: "proj-a", Payload: `{"id":"1"}`},
				{Seq: 2, Entity: "obs", EntityKey: "k2", Op: "upsert", Project: "proj-a", Payload: `{"id":"2"}`},
			}
			tr := newFakeTransport()
			tr.pushResult = tt.pushResult
			mgr := New(ls, tr, DefaultConfig())

			err := mgr.push(context.Background())
			if err == nil {
				t.Fatal("expected push to fail on accepted seq mismatch")
			}
			if !strings.Contains(err.Error(), tt.wantErrPiece) {
				t.Fatalf("expected error to contain %q, got %q", tt.wantErrPiece, err.Error())
			}
			ls.mu.Lock()
			acked := append([]int64(nil), ls.ackedSeqs...)
			ls.mu.Unlock()
			if len(acked) != 0 {
				t.Fatalf("expected no ack on accepted seq mismatch, got %v", acked)
			}
		})
	}
}

func TestManagerPushDoesNotAckWhenTransportFails(t *testing.T) {
	ls := newFakeLocalStore()
	ls.mutations = []store.SyncMutation{
		{Seq: 1, Entity: "obs", EntityKey: "k1", Op: "upsert", Project: "proj-a", Payload: `{"id":"1"}`},
	}
	tr := newFakeTransport()
	tr.pushErr = errors.New("transport down")
	mgr := New(ls, tr, DefaultConfig())

	if err := mgr.push(context.Background()); err == nil {
		t.Fatal("expected push to fail")
	}

	ls.mu.Lock()
	acked := append([]int64(nil), ls.ackedSeqs...)
	ls.mu.Unlock()
	if len(acked) != 0 {
		t.Fatalf("expected no ack after failed transport push, got %v", acked)
	}
}

// ─── Phase + lifecycle tests ──────────────────────────────────────────────────

func TestManagerPhaseTransitions(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	mgr := New(ls, tr, cfg)
	if mgr.Status().Phase != PhaseIdle {
		t.Fatalf("initial phase should be idle, got %q", mgr.Status().Phase)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr.Status().Phase == PhaseHealthy {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected PhaseHealthy after successful cycle, got %q", mgr.Status().Phase)
}

func TestManagerPushFailedPhase(t *testing.T) {
	ls := newFakeLocalStore()
	ls.mutations = []store.SyncMutation{{Seq: 1, Entity: "obs", EntityKey: "k1", Project: "proj-a"}}
	tr := newFakeTransport()
	tr.pushErr = errors.New("push failed")
	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)
	mgr.NotifyDirty()

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr.Status().Phase == PhasePushFailed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected PhasePushFailed, got %q", mgr.Status().Phase)
}

func TestManagerPullFailedPhase(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	tr.pullErr = errors.New("pull failed")
	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)
	mgr.NotifyDirty()

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr.Status().Phase == PhasePullFailed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected PhasePullFailed, got %q", mgr.Status().Phase)
}

func TestManagerRepairableFailureStoresUpgradeGuidance(t *testing.T) {
	ls := newFakeLocalStore()
	ls.mutations = []store.SyncMutation{{Seq: 1, Entity: "obs", EntityKey: "k1", Project: "proj-a"}}
	tr := newFakeTransport()
	tr.pushErr = fakeRepairableCloudError{msg: "invalid upsert payload: observations[0].directory is required"}
	cfg := DefaultConfig()
	cfg.TargetKey = "cloud:proj-a"

	mgr := New(ls, tr, cfg)
	mgr.cycle(context.Background())

	status := mgr.Status()
	if status.Phase != PhasePushFailed {
		t.Fatalf("expected PhasePushFailed, got %q", status.Phase)
	}
	if !strings.Contains(status.LastError, "invalid upsert payload") {
		t.Fatalf("expected original error preserved, got %q", status.LastError)
	}
	for _, want := range []string{
		"Known repairable cloud sync failure detected.",
		"engram cloud upgrade doctor --project proj-a",
		"engram cloud upgrade repair --project proj-a --apply",
	} {
		if !strings.Contains(status.LastError, want) {
			t.Fatalf("expected status.LastError to contain %q, got %q", want, status.LastError)
		}
	}
}

func TestManagerStopForUpgradeDisabled(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	mgr := New(ls, tr, DefaultConfig())

	if err := mgr.StopForUpgrade("test-project"); err != nil {
		t.Fatalf("StopForUpgrade: %v", err)
	}
	if mgr.Status().Phase != PhaseDisabled {
		t.Fatalf("expected PhaseDisabled, got %q", mgr.Status().Phase)
	}
}

func TestManagerResumeAfterUpgrade(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	mgr := New(ls, tr, DefaultConfig())

	_ = mgr.StopForUpgrade("test-project")
	if mgr.Status().Phase != PhaseDisabled {
		t.Fatal("expected PhaseDisabled after StopForUpgrade")
	}
	if err := mgr.ResumeAfterUpgrade("test-project"); err != nil {
		t.Fatalf("ResumeAfterUpgrade: %v", err)
	}
	if mgr.Status().Phase != PhaseIdle {
		t.Fatalf("expected PhaseIdle after ResumeAfterUpgrade, got %q", mgr.Status().Phase)
	}
}

func TestManagerResumeWithoutStop(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	mgr := New(ls, tr, DefaultConfig())

	// ResumeAfterUpgrade without prior stop is a no-op.
	if err := mgr.ResumeAfterUpgrade("test-project"); err != nil {
		t.Fatalf("ResumeAfterUpgrade on non-disabled manager: %v", err)
	}
	if mgr.Status().Phase != PhaseIdle {
		t.Fatalf("phase should remain idle, got %q", mgr.Status().Phase)
	}
}

// ─── Backoff tests ────────────────────────────────────────────────────────────

func TestManagerBackoffExponentialGrowth(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BaseBackoff = 1 * time.Second
	cfg.MaxBackoff = 5 * time.Minute
	mgr := &Manager{cfg: cfg}

	prev := time.Duration(0)
	for i := 1; i <= 8; i++ {
		d := mgr.computeBackoff(i)
		if d > cfg.MaxBackoff {
			t.Fatalf("failure %d: backoff %v exceeds max %v", i, d, cfg.MaxBackoff)
		}
		if i > 1 && prev > 0 {
			ratio := float64(d) / float64(prev)
			if ratio < 0.4 || ratio > 5.0 {
				t.Fatalf("failure %d: ratio %.2f out of [0.4,5.0] prev=%v cur=%v", i, ratio, prev, d)
			}
		}
		prev = d
	}
}

func TestManagerBackoffJitterBounds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BaseBackoff = 4 * time.Second
	cfg.MaxBackoff = 5 * time.Minute
	mgr := &Manager{cfg: cfg}

	sawBelowBase := false
	for i := 0; i < 500; i++ {
		d := mgr.computeBackoff(1)
		if d < 3*time.Second || d > 5*time.Second {
			t.Fatalf("jitter out of [3s,5s]: got %v at iteration %d", d, i)
		}
		if d < 4*time.Second {
			sawBelowBase = true
		}
	}
	if !sawBelowBase {
		t.Fatal("jitter never produced a result below base (4s) in 500 iterations; ±25% jitter must include negative direction")
	}
}

func TestManagerBackoffCeiling(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BaseBackoff = 1 * time.Second
	cfg.MaxBackoff = 5 * time.Minute
	mgr := &Manager{cfg: cfg}

	d := mgr.computeBackoff(10)
	if d > cfg.MaxBackoff {
		t.Fatalf("backoff exceeds ceiling: %v > %v", d, cfg.MaxBackoff)
	}
}

func TestManagerStopBeforeRun(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	mgr := New(ls, tr, DefaultConfig())
	// Stop before Run should not block.
	done := make(chan struct{})
	go func() {
		mgr.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop before Run blocked unexpectedly")
	}
}

func TestManagerRunContextCancel(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	cfg := DefaultConfig()
	cfg.PollInterval = 10 * time.Second
	cfg.DebounceDuration = 10 * time.Second

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		mgr.Run(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not exit after context cancel")
	}
}

func TestManagerNotifyDirtyOneCycle(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	cfg := DefaultConfig()
	cfg.DebounceDuration = 20 * time.Millisecond
	cfg.PollInterval = 10 * time.Second

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)
	mgr.NotifyDirty()

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr.Status().Phase == PhaseHealthy {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected PhaseHealthy after dirty notification, got %q", mgr.Status().Phase)
}

func TestManagerStopWaitsGoroutine(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	mgr := New(ls, tr, cfg)
	ctx := context.Background()
	go mgr.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		mgr.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop did not return after context cancel")
	}
}

func TestManagerRunIsNotReentryable(t *testing.T) {
	ls := newFakeLocalStore()
	tr := newFakeTransport()
	cfg := DefaultConfig()
	cfg.PollInterval = 10 * time.Second
	cfg.DebounceDuration = 10 * time.Second

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.Run(ctx)
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		mgr.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second Run call did not return quickly — re-entry not guarded")
	}
}

// ─── BW5: Auth/policy error surfacing ────────────────────────────────────────

type fakeAuthErr struct{ code int }

func (e *fakeAuthErr) Error() string         { return fmt.Sprintf("transport: status %d", e.code) }
func (e *fakeAuthErr) IsAuthFailure() bool   { return e.code == 401 }
func (e *fakeAuthErr) IsPolicyFailure() bool { return e.code == 403 }

type errTransport struct {
	pushErr error
	pullErr error
}

func (t *errTransport) PushMutations(_ []MutationEntry) (*PushMutationsResult, error) {
	if t.pushErr != nil {
		return nil, t.pushErr
	}
	return &PushMutationsResult{AcceptedSeqs: []int64{}}, nil
}

func (t *errTransport) PullMutations(_ int64, _ int) (*PullMutationsResponse, error) {
	if t.pullErr != nil {
		return nil, t.pullErr
	}
	return &PullMutationsResponse{Mutations: []PulledMutation{}}, nil
}

func TestManagerSurfacesAuthRequiredOn401(t *testing.T) {
	ls := newFakeLocalStore()
	ls.mutations = []store.SyncMutation{{Seq: 1, Entity: "obs", EntityKey: "k1", Project: "proj-a"}}

	tr := &errTransport{pushErr: &fakeAuthErr{code: 401}}
	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)
	mgr.NotifyDirty()

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		st := mgr.Status()
		if st.Phase == PhasePushFailed || st.Phase == PhaseBackoff {
			if st.ReasonCode != "auth_required" {
				t.Fatalf("expected ReasonCode=auth_required for 401, got %q", st.ReasonCode)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected PhasePushFailed/PhaseBackoff with auth_required, got phase=%q code=%q",
		mgr.Status().Phase, mgr.Status().ReasonCode)
}

func TestManagerSurfacesPolicyForbiddenOn403(t *testing.T) {
	ls := newFakeLocalStore()
	ls.mutations = []store.SyncMutation{{Seq: 1, Entity: "obs", EntityKey: "k1", Project: "proj-a"}}

	tr := &errTransport{pushErr: &fakeAuthErr{code: 403}}
	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)
	mgr.NotifyDirty()

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		st := mgr.Status()
		if st.Phase == PhasePushFailed || st.Phase == PhaseBackoff {
			if st.ReasonCode != "policy_forbidden" {
				t.Fatalf("expected ReasonCode=policy_forbidden for 403, got %q", st.ReasonCode)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected PhasePushFailed/PhaseBackoff with policy_forbidden, got phase=%q code=%q",
		mgr.Status().Phase, mgr.Status().ReasonCode)
}

func TestManagerBlocksWhenOnlyNonEnrolledPendingMutationsRemain(t *testing.T) {
	ls := newFakeLocalStore()
	ls.nonEnrolledCounts = []store.PendingSyncMutationProjectCount{
		{Project: "alpha", Count: 2},
		{Project: "beta", Count: 1},
	}
	tr := newFakeTransport()
	mgr := New(ls, tr, DefaultConfig())

	mgr.cycle(context.Background())

	if got := atomic.LoadInt32(&tr.pushCalls); got != 0 {
		t.Fatalf("expected no push calls for non-enrolled pending mutations, got %d", got)
	}
	if got := atomic.LoadInt32(&tr.pullCalls); got != 0 {
		t.Fatalf("expected blocked cycle to skip pull, got %d", got)
	}
	st := mgr.Status()
	if st.Phase != PhasePushFailed {
		t.Fatalf("expected push_failed status, got %q", st.Phase)
	}
	if st.ReasonCode != "non_enrolled_pending_mutations" {
		t.Fatalf("expected non-enrolled reason code, got %q", st.ReasonCode)
	}
	for _, want := range []string{"alpha=2", "beta=1", "engram cloud enroll <project>"} {
		if !strings.Contains(st.ReasonMessage, want) {
			t.Fatalf("expected reason message to contain %q, got %q", want, st.ReasonMessage)
		}
	}
	if ls.blockedReason != st.ReasonCode {
		t.Fatalf("expected blocked state persisted, got reason=%q", ls.blockedReason)
	}
}

// ─── Phase E: deferred replay tests ─────────────────────────────────────────

// DeferredRow is a minimal test representation of a sync_apply_deferred row.
type DeferredRow struct {
	SyncID      string
	Entity      string
	Payload     string
	RetryCount  int
	ApplyStatus string
}

// fakeLocalStoreWithDeferred extends fakeLocalStore with replay support.
type fakeLocalStoreWithDeferred struct {
	fakeLocalStore
	deferredRows         []DeferredRow
	replayDeferredCalled bool
	markDeadCalled       bool
	replayErr            error
}

func (s *fakeLocalStoreWithDeferred) ReplayDeferred() (store.ReplayDeferredResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replayDeferredCalled = true

	var res store.ReplayDeferredResult
	for i := range s.deferredRows {
		row := &s.deferredRows[i]
		if row.ApplyStatus == "dead" {
			continue
		}
		res.Retried++
		if s.replayErr != nil {
			row.RetryCount++
			if row.RetryCount >= 5 {
				row.ApplyStatus = "dead"
				s.markDeadCalled = true
				res.Dead++
			} else {
				res.Failed++
			}
		} else {
			row.ApplyStatus = "applied"
			res.Succeeded++
		}
	}
	return res, nil
}

func (s *fakeLocalStoreWithDeferred) CountDeferredAndDead() (deferred, dead int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, row := range s.deferredRows {
		switch row.ApplyStatus {
		case "deferred":
			deferred++
		case "dead":
			dead++
		}
	}
	return deferred, dead, nil
}

func TestReplayDeferred_IsCalledDuringPull(t *testing.T) {
	ls := &fakeLocalStoreWithDeferred{fakeLocalStore: *newFakeLocalStore()}
	tr := newFakeTransport()
	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		ls.mu.Lock()
		called := ls.replayDeferredCalled
		ls.mu.Unlock()
		if called {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("ReplayDeferred was not called during pull cycle")
}

func TestReplayDeferred_DeadAfterFiveRetries(t *testing.T) {
	ls := &fakeLocalStoreWithDeferred{fakeLocalStore: *newFakeLocalStore()}
	tr := newFakeTransport()
	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	ls.mu.Lock()
	ls.deferredRows = []DeferredRow{{
		SyncID:      "rel-dead",
		Entity:      "relation",
		Payload:     `{"sync_id":"rel-dead"}`,
		RetryCount:  4,
		ApplyStatus: "deferred",
	}}
	ls.replayErr = store.ErrRelationFKMissing
	ls.mu.Unlock()

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		ls.mu.Lock()
		called := ls.replayDeferredCalled
		dead := ls.markDeadCalled
		ls.mu.Unlock()
		if called && dead {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected MarkApplyDead called after retry_count reached 5")
}

func TestReplayDeferred_DeadRowNotRetried(t *testing.T) {
	ls := &fakeLocalStoreWithDeferred{fakeLocalStore: *newFakeLocalStore()}
	tr := newFakeTransport()
	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	ls.mu.Lock()
	ls.deferredRows = []DeferredRow{{
		SyncID:      "rel-already-dead",
		Entity:      "relation",
		Payload:     `{"sync_id":"rel-already-dead"}`,
		RetryCount:  5,
		ApplyStatus: "dead",
	}}
	ls.mu.Unlock()

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		ls.mu.Lock()
		called := ls.replayDeferredCalled
		ls.mu.Unlock()
		if called {
			ls.mu.Lock()
			appliedCount := len(ls.appliedMuts)
			ls.mu.Unlock()
			if appliedCount != 0 {
				t.Fatalf("dead row should never be applied; got %d applied mutations", appliedCount)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("ReplayDeferred was not called during pull cycle")
}

func TestPull_LegacyEntityNonFKError_StillHalts(t *testing.T) {
	ls := &fakeLocalStoreWithDeferred{fakeLocalStore: *newFakeLocalStore()}
	tr := newFakeTransport()

	tr.mu.Lock()
	tr.pullResult = &PullMutationsResponse{
		Mutations: []PulledMutation{{
			Seq:     10,
			Entity:  "observation",
			Op:      "upsert",
			Payload: []byte(`{"sync_id":"obs-fail","title":"test"}`),
		}},
		HasMore: false,
	}
	tr.mu.Unlock()

	ls.mu.Lock()
	ls.pullErr = errors.New("legacy apply error (non-FK)")
	ls.mu.Unlock()

	cfg := DefaultConfig()
	cfg.DebounceDuration = 10 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	mgr := New(ls, tr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go mgr.Run(ctx)

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		st := mgr.Status()
		if st.Phase == PhasePullFailed {
			ls.mu.Lock()
			cursorSeq := ls.syncState.LastPulledSeq
			ls.mu.Unlock()
			if cursorSeq != 0 {
				t.Fatalf("cursor advanced to %d despite legacy pull error; expected 0", cursorSeq)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected PhasePullFailed for legacy non-FK error, got %q", mgr.Status().Phase)
}

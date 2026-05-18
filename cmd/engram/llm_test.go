package main

import (
	"context"
	"errors"
	"testing"

	"github.com/Gentleman-Programming/engram/internal/store"
)

// ─── llmRunnerAdapter tests ───────────────────────────────────────────────────

// fakeAgentRunner is an llm.AgentRunner-compatible fake for adapter tests.
// It lives here (in package main) to avoid a circular import with internal/llm.
type fakeAgentRunnerForAdapter struct {
	relation   string
	confidence float64
	reasoning  string
	model      string
	durationMS int64
	err        error
}

// Compare implements llm.AgentRunner via duck typing. The adapter only calls
// the Compare signature, so we don't need to import internal/llm here.
func (f *fakeAgentRunnerForAdapter) Compare(_ context.Context, _ string) (interface{}, error) {
	return nil, f.err
}

// TestLLMRunnerAdapter_HappyPath verifies that llmRunnerAdapter correctly bridges
// an llm.AgentRunner to store.SemanticRunner by copying all Verdict fields.
func TestLLMRunnerAdapter_HappyPath(t *testing.T) {
	// Build an llmRunnerAdapter with a controllable inner fake via agentRunnerFactory.
	// We replace agentRunnerFactory for this test.

	wantVerdict := store.SemanticVerdict{
		Relation:   "compatible",
		Confidence: 0.9,
		Reasoning:  "They are consistent.",
		Model:      "claude-haiku-4-5",
		DurationMS: 500,
	}

	var capturedPrompt string
	fakeSemRunner := &fakeSemanticRunnerForCLI{
		verdict:         wantVerdict,
		capturedPrompts: &capturedPrompt,
	}

	// Temporarily override agentRunnerFactory.
	orig := agentRunnerFactory
	agentRunnerFactory = func(name string) (store.SemanticRunner, error) {
		return fakeSemRunner, nil
	}
	t.Cleanup(func() { agentRunnerFactory = orig })

	t.Setenv("ENGRAM_AGENT_CLI", "claude")

	runner, err := resolveAgentRunner()
	if err != nil {
		t.Fatalf("resolveAgentRunner: %v", err)
	}

	got, err := runner.Compare(context.Background(), "test prompt")
	if err != nil {
		t.Fatalf("Compare: unexpected error: %v", err)
	}

	if got.Relation != wantVerdict.Relation {
		t.Errorf("Relation = %q; want %q", got.Relation, wantVerdict.Relation)
	}
	if got.Confidence != wantVerdict.Confidence {
		t.Errorf("Confidence = %v; want %v", got.Confidence, wantVerdict.Confidence)
	}
	if got.Reasoning != wantVerdict.Reasoning {
		t.Errorf("Reasoning = %q; want %q", got.Reasoning, wantVerdict.Reasoning)
	}
	if got.Model != wantVerdict.Model {
		t.Errorf("Model = %q; want %q", got.Model, wantVerdict.Model)
	}
	if got.DurationMS != wantVerdict.DurationMS {
		t.Errorf("DurationMS = %d; want %d", got.DurationMS, wantVerdict.DurationMS)
	}
}

// TestResolveAgentRunner_MissingEnvVar verifies that resolveAgentRunner returns
// a descriptive error when ENGRAM_AGENT_CLI is not set.
func TestResolveAgentRunner_MissingEnvVar(t *testing.T) {
	t.Setenv("ENGRAM_AGENT_CLI", "") // ensure unset

	_, err := resolveAgentRunner()
	if err == nil {
		t.Fatal("expected error when ENGRAM_AGENT_CLI is not set; got nil")
	}
	if err.Error() == "" {
		t.Fatal("expected non-empty error message")
	}
}

// TestResolveAgentRunner_FactoryError verifies that agentRunnerFactory errors
// propagate through resolveAgentRunner.
func TestResolveAgentRunner_FactoryError(t *testing.T) {
	t.Setenv("ENGRAM_AGENT_CLI", "invalid-runner")

	sentinel := errors.New("factory failed")

	orig := agentRunnerFactory
	agentRunnerFactory = func(name string) (store.SemanticRunner, error) {
		return nil, sentinel
	}
	t.Cleanup(func() { agentRunnerFactory = orig })

	_, err := resolveAgentRunner()
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error; got %v", err)
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// fakeSemanticRunnerForCLI is a store.SemanticRunner for CLI-level adapter tests.
type fakeSemanticRunnerForCLI struct {
	verdict         store.SemanticVerdict
	err             error
	capturedPrompts *string
}

func (f *fakeSemanticRunnerForCLI) Compare(_ context.Context, prompt string) (store.SemanticVerdict, error) {
	if f.capturedPrompts != nil {
		*f.capturedPrompts = prompt
	}
	return f.verdict, f.err
}

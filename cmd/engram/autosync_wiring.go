// autosync_wiring.go — CLIENT-SIDE autosync startup wiring.
//
// Provides tryStartAutosync, which is called from cmdServe and cmdMCP when
// ENGRAM_CLOUD_AUTOSYNC=1 and both ENGRAM_CLOUD_TOKEN and ENGRAM_CLOUD_SERVER
// are set. Never fatal — autosync is optional.
//
// Server-side cloud packages (cloudserver, cloudstore, dashboard) are NOT
// imported here. This file only wires the client-side mutation transport and
// the autosync Manager into the long-running serve/mcp processes.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Gentleman-Programming/engram/internal/cloud"
	"github.com/Gentleman-Programming/engram/internal/cloud/autosync"
	"github.com/Gentleman-Programming/engram/internal/store"
)

// ─── Cloud config (client-side, stored in DataDir/cloud.json) ────────────────

// cloudConfig holds the persisted cloud runtime configuration.
// Only ServerURL and Token are relevant for client-side autosync.
type cloudConfig struct {
	ServerURL string `json:"server_url"`
	Token     string `json:"token"`
}

func cloudConfigPath(cfg store.Config) string {
	return filepath.Join(cfg.DataDir, "cloud.json")
}

// loadCloudConfig reads cloud.json from DataDir. Returns nil, nil when the file
// does not exist (unconfigured is not an error).
func loadCloudConfig(cfg store.Config) (*cloudConfig, error) {
	path := cloudConfigPath(cfg)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cc cloudConfig
	if err := json.Unmarshal(b, &cc); err != nil {
		return nil, err
	}
	return &cc, nil
}

// resolveCloudRuntimeConfig returns the effective cloud config for the current
// process. Persisted tokens in cloud.json are ignored at runtime; only
// ENGRAM_CLOUD_TOKEN and ENGRAM_CLOUD_SERVER environment variables are used.
func resolveCloudRuntimeConfig(cfg store.Config) (*cloudConfig, error) {
	cc, err := loadCloudConfig(cfg)
	if err != nil {
		return nil, err
	}
	if cc == nil {
		cc = &cloudConfig{}
	}
	// Legacy persisted tokens are intentionally ignored — runtime auth must
	// come from environment variables only.
	cc.Token = ""
	if v := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_SERVER")); v != "" {
		cc.ServerURL = v
	}
	if v := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_TOKEN")); v != "" {
		cc.Token = v
	}
	return cc, nil
}

// ─── Autosync status interfaces ───────────────────────────────────────────────

// autosyncStatusProvider is the minimal interface for consumers that only
// need to read the autosync status (not run or stop the manager).
type autosyncStatusProvider interface {
	Status() autosync.Status
}

// ─── Mutation transport adapter ───────────────────────────────────────────────

// mutationTransportAdapter bridges cloud.MutationTransport (which returns []int64 from
// PushMutations) to autosync.CloudTransport (which expects *autosync.PushMutationsResult).
type mutationTransportAdapter struct {
	remote *cloud.MutationTransport
}

func (a *mutationTransportAdapter) PushMutations(entries []autosync.MutationEntry) (*autosync.PushMutationsResult, error) {
	cloudEntries := make([]cloud.MutationEntry, len(entries))
	for i, e := range entries {
		cloudEntries[i] = cloud.MutationEntry{
			Project:   e.Project,
			Entity:    e.Entity,
			EntityKey: e.EntityKey,
			Op:        e.Op,
			Payload:   e.Payload,
		}
	}
	seqs, err := a.remote.PushMutations(cloudEntries)
	if err != nil {
		return nil, err
	}
	return &autosync.PushMutationsResult{AcceptedSeqs: seqs}, nil
}

func (a *mutationTransportAdapter) PullMutations(sinceSeq int64, limit int) (*autosync.PullMutationsResponse, error) {
	resp, err := a.remote.PullMutations(sinceSeq, limit)
	if err != nil {
		return nil, err
	}
	mutations := make([]autosync.PulledMutation, len(resp.Mutations))
	for i, m := range resp.Mutations {
		mutations[i] = autosync.PulledMutation{
			Seq:        m.Seq,
			Entity:     m.Entity,
			EntityKey:  m.EntityKey,
			Op:         m.Op,
			Payload:    m.Payload,
			OccurredAt: m.OccurredAt,
		}
	}
	return &autosync.PullMutationsResponse{
		Mutations: mutations,
		HasMore:   resp.HasMore,
		LatestSeq: resp.LatestSeq,
	}, nil
}

// ─── tryStartAutosync ─────────────────────────────────────────────────────────

// tryStartAutosync starts the autosync Manager if ENGRAM_CLOUD_AUTOSYNC=1 and
// both ENGRAM_CLOUD_TOKEN and ENGRAM_CLOUD_SERVER are present.
//
// Only exact "1" is accepted for ENGRAM_CLOUD_AUTOSYNC.
// Missing token or server URL → log+skip (never fatal).
//
// Returns (status provider, stop func). Both may be nil if autosync is disabled
// or config is missing. The stop func must be called before process exit to
// release the sync lease.
func tryStartAutosync(ctx context.Context, s store.Store, cfg store.Config) (autosyncStatusProvider, func()) {
	if strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_AUTOSYNC")) != "1" {
		return nil, nil
	}

	cc, err := resolveCloudRuntimeConfig(cfg)
	if err != nil {
		log.Printf("[autosync] ERROR: cannot read cloud config: %v; autosync disabled", err)
		return nil, nil
	}

	token := strings.TrimSpace(cc.Token)
	serverURL := strings.TrimSpace(cc.ServerURL)

	if token == "" {
		log.Printf("[autosync] ERROR: ENGRAM_CLOUD_TOKEN is required when ENGRAM_CLOUD_AUTOSYNC=1; autosync disabled")
		return nil, nil
	}
	if serverURL == "" {
		log.Printf("[autosync] ERROR: ENGRAM_CLOUD_SERVER is required when ENGRAM_CLOUD_AUTOSYNC=1; autosync disabled")
		return nil, nil
	}

	remoteMT, err := cloud.NewMutationTransport(serverURL, token)
	if err != nil {
		log.Printf("[autosync] ERROR: invalid server URL %q: %v; autosync disabled", serverURL, err)
		return nil, nil
	}

	transport := &mutationTransportAdapter{remote: remoteMT}
	mgrCfg := autosync.DefaultConfig()
	mgr := autosync.New(s, transport, mgrCfg)

	go mgr.Run(ctx)
	log.Printf("[autosync] started (server=%s)", serverURL)
	return mgr, mgr.Stop
}

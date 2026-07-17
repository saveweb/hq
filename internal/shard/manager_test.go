package shard

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const managerNow = int64(1_780_000_000)

type managerFixture struct {
	manager     *Manager
	signer      *access.Signer
	key         protocol.SigningKey
	localClock  int64
	signerClock int64
}

func newManagerFixture(t *testing.T, configure ...func(*ManagerConfig)) *managerFixture {
	t.Helper()
	value := &managerFixture{localClock: managerNow - 10, signerClock: managerNow}
	publicKey, privateKey, err := access.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	value.signer, err = access.NewSigner("https://tracker.test", "key-1", privateKey, func() int64 { return value.signerClock })
	if err != nil {
		t.Fatal(err)
	}
	value.key = protocol.SigningKey{
		KeyID: "key-1", Algorithm: "EdDSA",
		PublicKeyEd25519: base64.RawURLEncoding.EncodeToString(ed25519.PublicKey(publicKey)),
		NotBefore:        managerNow - 60, NotAfter: managerNow + 3600,
	}
	config := ManagerConfig{
		AgentID: "shard-agent-1", Issuer: "https://tracker.test", DataDir: t.TempDir(),
		Clock: func() int64 { return value.localClock },
	}
	for _, apply := range configure {
		apply(&config)
	}
	value.manager, err = NewManager(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := value.manager.Close(); err != nil {
			t.Error(err)
		}
	})
	return value
}

func (f *managerFixture) heartbeat(generation, ownerLease int64, status string) protocol.AgentHeartbeatResponse {
	return protocol.AgentHeartbeatResponse{
		ServerTime: managerNow, HeartbeatAfterSeconds: 30,
		SigningKeys: []protocol.SigningKey{f.key},
		OwnerAssignments: []protocol.OwnerAssignment{{
			Route:  protocol.Route{ProjectID: "project-1", ShardID: "shard-1", Generation: generation},
			Status: status, OwnerLeaseExpiresAt: ownerLease,
		}},
	}
}

func (f *managerFixture) token(t *testing.T, generation int64) string {
	t.Helper()
	token, _, err := f.signer.Sign(access.Scope{
		WorkerAgentID: "worker-1", SessionID: "session-1", ProjectID: "project-1",
		ShardID: "shard-1", Generation: generation, OwnerAgentID: "shard-agent-1",
	}, managerNow+300, 300)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestManagerInstallsFenceAuthorizesAndRecoversGeneration(t *testing.T) {
	f := newManagerFixture(t)
	if err := f.manager.ApplyHeartbeat(context.Background(), f.heartbeat(1, managerNow+120, trackerStatusActive)); err != nil {
		t.Fatal(err)
	}
	if f.manager.Now() != managerNow {
		t.Fatalf("tracker-adjusted now = %d", f.manager.Now())
	}
	authorization, err := f.manager.Authorize(f.token(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := authorization.CheckRoute(protocol.SessionRoute{
		Route: protocol.Route{ProjectID: "project-1", ShardID: "shard-1", Generation: 1}, SessionID: "session-1",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := authorization.Store.Enqueue(context.Background(), 1, managerNow, []queue.JobSpec{{
		ID: "job-1", URL: "https://example.test/", Type: protocol.JobTypeSeed, Attrs: map[string]any{},
	}}); err != nil {
		t.Fatal(err)
	}
	claimed, err := authorization.Store.ClaimBatch(context.Background(), 1, managerNow, "session-1", nil, 1, 60)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}

	if err := f.manager.ApplyHeartbeat(context.Background(), f.heartbeat(2, managerNow+120, trackerStatusActive)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Authorize(f.token(t, 1)); !queueCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("old generation authorization = %v", err)
	}
	authorization, err = f.manager.Authorize(f.token(t, 2))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := authorization.Store.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Todo != 1 || stats.WIP != 0 {
		t.Fatalf("generation recovery stats = %+v", stats)
	}
}

func TestManagerEnforcesOwnerLeaseAndDrainStatus(t *testing.T) {
	f := newManagerFixture(t)
	if err := f.manager.ApplyHeartbeat(context.Background(), f.heartbeat(1, managerNow+20, trackerStatusDraining)); err != nil {
		t.Fatal(err)
	}
	authorization, err := f.manager.Authorize(f.token(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !queueCode(authorization.AllowsClaim(), protocol.ErrorShardNotActive) || authorization.AllowsMutation() != nil {
		t.Fatal("draining shard accepted claim or rejected completion")
	}
	f.localClock += 20
	if _, err := f.manager.Authorize(f.token(t, 1)); !queueCode(err, protocol.ErrorOwnerLeaseExpired) {
		t.Fatalf("expired owner lease authorization = %v", err)
	}
}

func TestManagerLocalClaimPauseKeepsMutationsAvailable(t *testing.T) {
	f := newManagerFixture(t)
	if err := f.manager.ApplyHeartbeat(context.Background(), f.heartbeat(1, managerNow+120, trackerStatusActive)); err != nil {
		t.Fatal(err)
	}
	f.manager.SetClaimsPaused(true)
	authorization, err := f.manager.Authorize(f.token(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !queueCode(authorization.AllowsClaim(), protocol.ErrorShardNotActive) || authorization.AllowsMutation() != nil {
		t.Fatal("local pause did not reject claims while retaining mutation access")
	}
	status, err := f.manager.RuntimeStatus(context.Background())
	if err != nil || !status.ClaimsPaused || len(status.Shards) != 1 {
		t.Fatalf("runtime status = %+v, %v", status, err)
	}
	f.manager.SetClaimsPaused(false)
	authorization, err = f.manager.Authorize(f.token(t, 1))
	if err != nil || authorization.AllowsClaim() != nil {
		t.Fatalf("resumed authorization = %+v, %v", authorization, err)
	}
}

func TestManagerRejectsWrongScopeInvalidTokenAndUnconfiguredSource(t *testing.T) {
	f := newManagerFixture(t)
	heartbeat := f.heartbeat(1, managerNow+120, trackerStatusLoading)
	source := "s3://bucket/source.jsonl.zst"
	format := "jobs-jsonl-zstd-v1"
	etag := "source-etag"
	downloadURL := "https://objects.test/source?signature=secret"
	expiresAt := managerNow + 60
	heartbeat.OwnerAssignments[0].SourceURI = &source
	heartbeat.OwnerAssignments[0].SourceFormat = &format
	heartbeat.OwnerAssignments[0].SourceETag = &etag
	heartbeat.OwnerAssignments[0].SourceDownloadURL = &downloadURL
	heartbeat.OwnerAssignments[0].SourceURLExpiresAt = &expiresAt
	if err := f.manager.ApplyHeartbeat(context.Background(), heartbeat); !queueCode(err, protocol.ErrorUnsupportedOperation) {
		t.Fatalf("source assignment error = %v", err)
	}

	heartbeat = f.heartbeat(1, managerNow+120, trackerStatusActive)
	if err := f.manager.ApplyHeartbeat(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Authorize("garbage"); !queueCode(err, protocol.ErrorInvalidAccessToken) {
		t.Fatalf("invalid token error = %v", err)
	}
	authorization, err := f.manager.Authorize(f.token(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	err = authorization.CheckRoute(protocol.SessionRoute{
		Route: protocol.Route{ProjectID: "project-1", ShardID: "other", Generation: 1}, SessionID: "session-1",
	})
	if !queueCode(err, protocol.ErrorInvalidAccessToken) {
		t.Fatalf("wrong route error = %v", err)
	}
}

func TestManagerLoadsSourceOnceAndReportsResult(t *testing.T) {
	reported := make(chan error, 2)
	var loads atomic.Int64
	f := newManagerFixture(t, func(config *ManagerConfig) {
		config.LoadSource = func(ctx context.Context, assignment protocol.OwnerAssignment, store queue.Store) error {
			loads.Add(1)
			_, err := store.Enqueue(ctx, assignment.Generation, managerNow, []queue.JobSpec{{
				ID: "source-job-1", URL: "https://example.test/", Type: protocol.JobTypeSeed,
				Attrs: map[string]any{},
			}})
			return err
		}
		config.ReportSource = func(_ context.Context, _ protocol.OwnerAssignment, loadError error) error {
			reported <- loadError
			return nil
		}
	})
	heartbeat := f.heartbeat(1, managerNow+120, trackerStatusLoading)
	source := "s3://bucket/source.jobs.jsonl.zst"
	format := "jobs-jsonl-zstd-v1"
	etag := "source-etag"
	downloadURL := "https://objects.test/source?signature=first"
	expiresAt := managerNow + 60
	heartbeat.OwnerAssignments[0].SourceURI = &source
	heartbeat.OwnerAssignments[0].SourceFormat = &format
	heartbeat.OwnerAssignments[0].SourceETag = &etag
	heartbeat.OwnerAssignments[0].SourceDownloadURL = &downloadURL
	heartbeat.OwnerAssignments[0].SourceURLExpiresAt = &expiresAt
	if err := f.manager.ApplyHeartbeat(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reported:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("source load result was not reported")
	}

	// Tracker heartbeats rotate the secret URL. The immutable source identity
	// remains URI + ETag + generation, so this must not trigger another load.
	rotated := "https://objects.test/source?signature=second"
	heartbeat.OwnerAssignments[0].SourceDownloadURL = &rotated
	if err := f.manager.ApplyHeartbeat(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if loads.Load() != 1 {
		t.Fatalf("source loads = %d, want 1", loads.Load())
	}
	status, err := f.manager.RuntimeStatus(context.Background())
	if err != nil || len(status.Shards) != 1 || status.Shards[0].Stats.Todo != 1 || status.Shards[0].LoadRunning {
		t.Fatalf("runtime status = %+v, %v", status, err)
	}
}

func queueCode(err error, code string) bool {
	var value *queue.Error
	return errors.As(err, &value) && value.Code == code
}

package sqlitequeue

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	testGeneration = int64(7)
	testNow        = int64(1000)
	testLeaseEnd   = int64(10000)
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "queue.sqlite"), queue.Identity{
		ProjectID: "project-test", ShardID: "shard-test", Generation: testGeneration,
	}, WithClock(func() int64 { return testNow }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	if err := store.SetFence(ctx, testGeneration, testNow, testLeaseEnd); err != nil {
		t.Fatal(err)
	}
	return store
}

func seedJob(id string) queue.JobSpec {
	return queue.JobSpec{
		ID: id, URL: "https://example.org/" + id, Type: protocol.JobTypeSeed,
		Attrs: map[string]any{},
	}
}

func requireCode(t *testing.T, err error, code string) *queue.Error {
	t.Helper()
	var domainError *queue.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("error %v is not a queue error", err)
	}
	if domainError.Code != code {
		t.Fatalf("error code = %q, want %q", domainError.Code, code)
	}
	return domainError
}

func claimOne(t *testing.T, store *Store, generation, now int64, session string) queue.ClaimedJob {
	t.Helper()
	jobs, err := store.ClaimBatch(context.Background(), generation, now, session, nil, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(jobs))
	}
	return jobs[0]
}

func TestEnqueueIsIdempotentAndRejectsIdentityConflict(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	job := seedJob("job-1")
	job.Attrs = map[string]any{"headers": map[string]any{"Accept": "text/html"}}

	result, err := store.Enqueue(ctx, testGeneration, testNow, []queue.JobSpec{job})
	if err != nil {
		t.Fatal(err)
	}
	if result.Inserted != 1 || result.Duplicate != 0 {
		t.Fatalf("first enqueue = %+v", result)
	}

	duplicate := job
	via := "https://origin.example/"
	duplicate.Via = &via
	duplicate.Hops = 5
	result, err = store.Enqueue(ctx, testGeneration, testNow+1, []queue.JobSpec{duplicate})
	if err != nil {
		t.Fatal(err)
	}
	if result.Inserted != 0 || result.Duplicate != 1 {
		t.Fatalf("duplicate enqueue = %+v", result)
	}

	conflict := job
	conflict.URL = "https://different.example/"
	_, err = store.Enqueue(ctx, testGeneration, testNow+2, []queue.JobSpec{conflict})
	requireCode(t, err, protocol.ErrorIdentityConflict)

	invalid := seedJob("job-nan")
	invalid.Attrs = map[string]any{"not_json": math.NaN()}
	_, err = store.Enqueue(ctx, testGeneration, testNow+2, []queue.JobSpec{invalid})
	requireCode(t, err, protocol.ErrorInvalidJob)

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Todo != 1 {
		t.Fatalf("stats = %+v, want one todo", stats)
	}
}

func TestDatabaseIdentityAndStatePersistAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "queue.sqlite")
	identity := queue.Identity{ProjectID: "project-test", ShardID: "shard-test", Generation: testGeneration}
	clock := func() int64 { return testNow }
	store, err := Open(ctx, path, identity, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetFence(ctx, testGeneration, testNow, testLeaseEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enqueue(ctx, testGeneration, testNow, []queue.JobSpec{seedJob("persistent")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, path, identity, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Todo != 1 || store.Identity() != identity {
		t.Fatalf("reopened store: identity=%+v stats=%+v", store.Identity(), stats)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(ctx, path, queue.Identity{
		ProjectID: "other-project", ShardID: identity.ShardID, Generation: identity.Generation,
	}, WithClock(clock))
	if err == nil {
		t.Fatal("opening a shard database with the wrong identity succeeded")
	}
}

func TestConcurrentClaimNeverDuplicatesAJob(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	jobs := make([]queue.JobSpec, 200)
	for i := range jobs {
		jobs[i] = seedJob(fmt.Sprintf("job-%03d", i))
	}
	if _, err := store.Enqueue(ctx, testGeneration, testNow, jobs); err != nil {
		t.Fatal(err)
	}

	claimed := make(chan queue.ClaimedJob, len(jobs))
	errorsFound := make(chan error, 8)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for {
				batch, err := store.ClaimBatch(ctx, testGeneration, testNow+1,
					fmt.Sprintf("session-%d", worker), nil, 13, 300)
				if err != nil {
					errorsFound <- err
					return
				}
				if len(batch) == 0 {
					return
				}
				for _, job := range batch {
					claimed <- job
				}
			}
		}(worker)
	}
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	close(claimed)

	seenJobs := make(map[string]string, len(jobs))
	seenAttempts := make(map[string]struct{}, len(jobs))
	for job := range claimed {
		if previous, exists := seenJobs[job.ID]; exists {
			t.Fatalf("job %s claimed twice as %s and %s", job.ID, previous, job.AttemptID)
		}
		if _, exists := seenAttempts[job.AttemptID]; exists {
			t.Fatalf("attempt ID %s reused", job.AttemptID)
		}
		seenJobs[job.ID] = job.AttemptID
		seenAttempts[job.AttemptID] = struct{}{}
	}
	if len(seenJobs) != len(jobs) {
		t.Fatalf("claimed %d unique jobs, want %d", len(seenJobs), len(jobs))
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.WIP != int64(len(jobs)) || stats.Todo != 0 {
		t.Fatalf("stats after concurrent claim = %+v", stats)
	}
}

func TestCompleteDiscoveryIsAtomicAndIdempotent(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	parent := seedJob("parent")
	existing := seedJob("existing")
	existing.Type = protocol.JobTypeAsset
	if _, err := store.Enqueue(ctx, testGeneration, testNow, []queue.JobSpec{parent, existing}); err != nil {
		t.Fatal(err)
	}
	claimedJobs, err := store.ClaimBatch(ctx, testGeneration, testNow+1, "session-complete",
		[]string{protocol.JobTypeSeed}, 1, 300)
	if err != nil || len(claimedJobs) != 1 {
		t.Fatalf("claim parent: jobs=%d err=%v", len(claimedJobs), err)
	}
	claimed := claimedJobs[0]
	if claimed.ID != parent.ID {
		t.Fatalf("claimed %q, want parent", claimed.ID)
	}

	code := 200
	uri := "artifact://result"
	item := queue.CompleteItem{
		JobID: claimed.ID, AttemptID: claimed.AttemptID,
		Outcome: queue.Outcome{Kind: "success", Code: &code, URI: &uri, Meta: map[string]any{}},
		DiscoveredJobs: []queue.JobSpec{{
			ID: existing.ID, URL: "https://different.example/", Type: protocol.JobTypeAsset,
			Attrs: map[string]any{},
		}},
	}
	results, err := store.CompleteBatch(ctx, testGeneration, testNow+2, "session-complete", []queue.CompleteItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != queue.ResultRejected || results[0].Error.Code != protocol.ErrorIdentityConflict {
		t.Fatalf("conflicting completion result = %+v", results[0])
	}
	stats, _ := store.Stats(ctx)
	if stats.WIP != 1 || stats.Todo != 1 || stats.Done != 0 {
		t.Fatalf("conflict changed parent or children: %+v", stats)
	}

	item.DiscoveredJobs = []queue.JobSpec{seedJob("discovered")}
	results, err = store.CompleteBatch(ctx, testGeneration, testNow+3, "session-complete", []queue.CompleteItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != queue.ResultApplied || results[0].JobStatus != queue.StatusDone {
		t.Fatalf("completion result = %+v", results[0])
	}
	stats, _ = store.Stats(ctx)
	if stats.Done != 1 || stats.Todo != 2 || stats.WIP != 0 {
		t.Fatalf("completion stats = %+v", stats)
	}

	results, err = store.CompleteBatch(ctx, testGeneration, testNow+4, "session-complete", []queue.CompleteItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != queue.ResultAlreadyApplied {
		t.Fatalf("completion retry = %+v", results[0])
	}

	item.Outcome.Kind = "skipped"
	results, err = store.CompleteBatch(ctx, testGeneration, testNow+5, "session-complete", []queue.CompleteItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Error.Code != protocol.ErrorAttemptAlreadyFinalized {
		t.Fatalf("changed completion retry = %+v", results[0])
	}
}

func TestCompleteBatchKeepsValidItemsWhenAnotherItemConflicts(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	existing := seedJob("existing")
	existing.Type = protocol.JobTypeAsset
	if _, err := store.Enqueue(ctx, testGeneration, testNow, []queue.JobSpec{
		seedJob("parent-a"), seedJob("parent-b"), existing,
	}); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimBatch(ctx, testGeneration, testNow+1, "session-batch",
		[]string{protocol.JobTypeSeed}, 2, 300)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim parents: jobs=%d err=%v", len(claimed), err)
	}
	items := []queue.CompleteItem{
		{
			JobID: claimed[0].ID, AttemptID: claimed[0].AttemptID,
			Outcome: queue.Outcome{Kind: "success", Meta: map[string]any{}},
			DiscoveredJobs: []queue.JobSpec{{
				ID: existing.ID, URL: "https://conflict.example/", Type: protocol.JobTypeAsset,
				Attrs: map[string]any{},
			}},
		},
		{
			JobID: claimed[1].ID, AttemptID: claimed[1].AttemptID,
			Outcome:        queue.Outcome{Kind: "success", Meta: map[string]any{}},
			DiscoveredJobs: []queue.JobSpec{seedJob("valid-child")},
		},
	}
	results, err := store.CompleteBatch(ctx, testGeneration, testNow+2, "session-batch", items)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != queue.ResultRejected || results[0].Error.Code != protocol.ErrorIdentityConflict {
		t.Fatalf("conflicting item = %+v", results[0])
	}
	if results[1].Status != queue.ResultApplied {
		t.Fatalf("valid item = %+v", results[1])
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.WIP != 1 || stats.Done != 1 || stats.Todo != 2 {
		t.Fatalf("partial batch stats = %+v", stats)
	}
}

func TestFailDistinguishesFailedFromResetExhausted(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if _, err := store.Enqueue(ctx, testGeneration, testNow, []queue.JobSpec{seedJob("retry"), seedJob("terminal")}); err != nil {
		t.Fatal(err)
	}

	retry := claimOne(t, store, testGeneration, testNow+1, "session-retry")
	failure := queue.ExecutionError{Code: "network_timeout", Message: "timeout", Details: map[string]any{}}
	results, err := store.FailBatch(ctx, testGeneration, testNow+2, "session-retry", 1, []queue.FailItem{{
		JobID: retry.ID, AttemptID: retry.AttemptID, Retryable: true, Error: failure,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].JobStatus != queue.StatusTodo {
		t.Fatalf("first retryable fail = %+v", results[0])
	}

	retry = claimOne(t, store, testGeneration, testNow+3, "session-retry")
	item := queue.FailItem{JobID: retry.ID, AttemptID: retry.AttemptID, Retryable: true, Error: failure}
	results, err = store.FailBatch(ctx, testGeneration, testNow+4, "session-retry", 1, []queue.FailItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].JobStatus != queue.StatusResetExhausted {
		t.Fatalf("exhausted retryable fail = %+v", results[0])
	}
	results, err = store.FailBatch(ctx, testGeneration, testNow+5, "session-retry", 1, []queue.FailItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != queue.ResultAlreadyApplied {
		t.Fatalf("terminal fail retry = %+v", results[0])
	}

	terminal := claimOne(t, store, testGeneration, testNow+6, "session-terminal")
	results, err = store.FailBatch(ctx, testGeneration, testNow+7, "session-terminal", 1, []queue.FailItem{{
		JobID: terminal.ID, AttemptID: terminal.AttemptID, Retryable: false,
		Error: queue.ExecutionError{Code: "policy_denied", Message: "blocked", Details: map[string]any{}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].JobStatus != queue.StatusFailed {
		t.Fatalf("non-retryable fail = %+v", results[0])
	}
	stats, _ := store.Stats(ctx)
	if stats.Failed != 1 || stats.ResetExhausted != 1 {
		t.Fatalf("terminal status counts = %+v", stats)
	}
}

func TestLeaseExtensionExpiryAndRecoveryFence(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if _, err := store.Enqueue(ctx, testGeneration, testNow, []queue.JobSpec{seedJob("leased")}); err != nil {
		t.Fatal(err)
	}
	claimed := claimOne(t, store, testGeneration, testNow+1, "session-lease")
	results, err := store.ExtendLeaseBatch(ctx, testGeneration, testNow+10, "session-lease", 600, []queue.AttemptRef{{
		JobID: claimed.ID, AttemptID: claimed.AttemptID,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].LeaseExpiresAt == nil || *results[0].LeaseExpiresAt != testNow+610 {
		t.Fatalf("extended lease result = %+v", results[0])
	}

	requeue, err := store.RequeueExpired(ctx, testGeneration, testNow+609, 3, 100)
	if err != nil || requeue.Requeued != 0 {
		t.Fatalf("early requeue = %+v, %v", requeue, err)
	}
	complete, err := store.CompleteBatch(ctx, testGeneration, testNow+610, "session-lease", []queue.CompleteItem{{
		JobID: claimed.ID, AttemptID: claimed.AttemptID,
		Outcome: queue.Outcome{Kind: "success", Meta: map[string]any{}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if complete[0].Status != queue.ResultRejected || complete[0].Error.Code != protocol.ErrorLeaseExpired {
		t.Fatalf("completion at lease boundary = %+v", complete[0])
	}
	requeue, err = store.RequeueExpired(ctx, testGeneration, testNow+610, 3, 100)
	if err != nil || requeue.Requeued != 1 {
		t.Fatalf("expired requeue = %+v, %v", requeue, err)
	}

	claimed = claimOne(t, store, testGeneration, testNow+611, "session-recovery")
	if err := store.SetFence(ctx, testGeneration+1, testNow+612, testLeaseEnd+1000); err != nil {
		t.Fatal(err)
	}
	stats, _ := store.Stats(ctx)
	if stats.Todo != 1 || stats.WIP != 0 {
		t.Fatalf("generation recovery did not requeue WIP: %+v", stats)
	}
	var resetCount int
	if err := store.db.QueryRowContext(ctx, "SELECT reset_count FROM jobs WHERE id = ?", claimed.ID).Scan(&resetCount); err != nil {
		t.Fatal(err)
	}
	if resetCount != 1 {
		t.Fatalf("recovery changed reset count to %d, want existing value 1", resetCount)
	}
	_, err = store.ClaimBatch(ctx, testGeneration, testNow+613, "session-stale", nil, 1, 300)
	requireCode(t, err, protocol.ErrorStaleGeneration)
}

func TestOwnerLeaseIsFencedInsideTransaction(t *testing.T) {
	ctx := context.Background()
	commitNow := testNow
	store, err := Open(ctx, filepath.Join(t.TempDir(), "queue.sqlite"), queue.Identity{
		ProjectID: "project-test", ShardID: "shard-test", Generation: testGeneration,
	}, WithClock(func() int64 { return commitNow }))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	_, err = store.Enqueue(ctx, testGeneration, testNow, []queue.JobSpec{seedJob("job")})
	requireCode(t, err, protocol.ErrorOwnerLeaseExpired)
	if err := store.SetFence(ctx, testGeneration, testNow, testNow+10); err != nil {
		t.Fatal(err)
	}
	commitNow = testNow + 10
	_, err = store.Enqueue(ctx, testGeneration, testNow+9, []queue.JobSpec{seedJob("job")})
	requireCode(t, err, protocol.ErrorOwnerLeaseExpired)
	stats, statsErr := store.Stats(ctx)
	if statsErr != nil {
		t.Fatal(statsErr)
	}
	if stats.Todo != 0 {
		t.Fatalf("transaction crossing owner lease committed: %+v", stats)
	}
}

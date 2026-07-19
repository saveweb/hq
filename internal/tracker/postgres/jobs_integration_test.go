package postgres_test

import (
	"context"
	"os"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/internal/tracker/postgres"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestPostgresProjectQueueContract(t *testing.T) {
	databaseURL := os.Getenv("HQ_TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("HQ_TEST_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	store, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("idempotent migration: %v", err)
	}

	const now = int64(1_780_100_000)
	if err := store.PutUserAndToken(ctx, tracker.User{ID: "queue-worker", Status: tracker.UserStatusActive, Roles: map[string]bool{tracker.RoleWorker: true}}, "queue-worker-token", now); err != nil {
		t.Fatal(err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "queue-project", Status: tracker.ProjectStatusActive}, now); err != nil {
		t.Fatal(err)
	}
	jobs := []protocol.JobSpecV1{{ID: "job-1", URL: "https://example.test/1", Type: protocol.JobTypeSeed, Via: nil, Attrs: map[string]any{"source": "test"}}, {ID: "job-2", URL: "https://example.test/2", Via: nil}}
	inserted, err := store.EnqueueProjectJobs(ctx, "queue-project", jobs, now)
	if err != nil || inserted != 2 {
		t.Fatalf("enqueue = %d, %v", inserted, err)
	}
	inserted, err = store.EnqueueProjectJobs(ctx, "queue-project", jobs, now+1)
	if err != nil || inserted != 0 {
		t.Fatalf("idempotent enqueue = %d, %v", inserted, err)
	}
	conflict := append([]protocol.JobSpecV1(nil), jobs[:1]...)
	conflict[0].URL = "https://example.test/different"
	if _, err := store.EnqueueProjectJobs(ctx, "queue-project", conflict, now+2); !tracker.IsCode(err, protocol.ErrorIdentityConflict) {
		t.Fatalf("identity conflict = %v", err)
	}

	claimed, err := store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-1", MaxJobs: 1, LeaseSeconds: 300}, now+3)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}
	item := claimed[0]
	receipt := protocol.WARCReceipt{ID: "receipt-1", Issuer: "https://warc.test", ObjectID: "warc/object-1", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 1234, AcceptedAt: now + 4, Signature: "test-signature"}
	completed, err := store.CompleteProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectCompleteRequest{WorkerID: "worker-process-1", Items: []protocol.ProjectCompleteItem{{JobID: item.ID, AttemptID: item.AttemptID, Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}}, WARCReceipts: []protocol.WARCReceipt{receipt}}}}, now+5)
	if err != nil || len(completed.Results) != 1 || completed.Results[0].Status != protocol.ItemStatusApplied {
		t.Fatalf("complete = %+v, %v", completed, err)
	}
	replayed, err := store.CompleteProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectCompleteRequest{WorkerID: "worker-process-1", Items: []protocol.ProjectCompleteItem{{JobID: item.ID, AttemptID: item.AttemptID, Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}}, WARCReceipts: []protocol.WARCReceipt{receipt}}}}, now+6)
	if err != nil || replayed.Results[0].Status != protocol.ItemStatusRejected || replayed.Results[0].Error.Code != protocol.ErrorStaleAttempt {
		t.Fatalf("replayed complete = %+v, %v", replayed, err)
	}

	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-1", MaxJobs: 1, LeaseSeconds: 300}, now+7)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("second claim = %+v, %v", claimed, err)
	}
	extended, err := store.ExtendProjectJobLeases(ctx, "queue-worker", "queue-project", protocol.ProjectExtendLeaseRequest{WorkerID: "worker-process-1", ExtendSeconds: 600, Items: []protocol.AttemptRef{{JobID: claimed[0].ID, AttemptID: claimed[0].AttemptID}}}, now+8)
	if err != nil || extended.Results[0].Status != protocol.ItemStatusApplied || *extended.Results[0].LeaseExpiresAt != now+608 {
		t.Fatalf("extend lease = %+v, %v", extended, err)
	}
	failed, err := store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-1", Items: []protocol.FailItem{{JobID: claimed[0].ID, AttemptID: claimed[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "retry", Details: protocol.Attrs{}}}}}, now+9)
	if err != nil || failed.Results[0].Status != protocol.ItemStatusApplied || *failed.Results[0].JobStatus != protocol.JobStatusTodo {
		t.Fatalf("retryable fail = %+v, %v", failed, err)
	}

	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-1", MaxJobs: 1, LeaseSeconds: 1, AcceptTypes: []string{protocol.JobTypeSeed}}, now+10)
	if err != nil || len(claimed) != 1 || claimed[0].Type != protocol.JobTypeSeed {
		t.Fatalf("normalized claim = %+v, %v", claimed, err)
	}
	expiredAttempt := claimed[0]
	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-2", MaxJobs: 1, LeaseSeconds: 30}, now+12)
	if err != nil || len(claimed) != 1 || claimed[0].AttemptID == expiredAttempt.AttemptID {
		t.Fatalf("reclaim expired lease = %+v, %v", claimed, err)
	}
	stale, err := store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-1", Items: []protocol.FailItem{{JobID: expiredAttempt.ID, AttemptID: expiredAttempt.AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "stale", Details: protocol.Attrs{}}}}}, now+13)
	if err != nil || stale.Results[0].Status != protocol.ItemStatusRejected {
		t.Fatalf("stale expired attempt = %+v, %v", stale, err)
	}
	failed, err = store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-2", Items: []protocol.FailItem{{JobID: claimed[0].ID, AttemptID: claimed[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "retry", Details: protocol.Attrs{}}}}}, now+14)
	if err != nil || *failed.Results[0].JobStatus != protocol.JobStatusTodo {
		t.Fatalf("third reset = %+v, %v", failed, err)
	}
	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-2", MaxJobs: 1, LeaseSeconds: 30}, now+15)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("final claim = %+v, %v", claimed, err)
	}
	failed, err = store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-2", Items: []protocol.FailItem{{JobID: claimed[0].ID, AttemptID: claimed[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "exhaust", Details: protocol.Attrs{}}}}}, now+16)
	if err != nil || *failed.Results[0].JobStatus != protocol.JobStatusResetExhausted {
		t.Fatalf("reset exhaustion = %+v, %v", failed, err)
	}

	counts, err := store.ProjectJobCounts(ctx, "queue-project")
	if err != nil || counts[protocol.JobStatusDone] != 1 || counts[protocol.JobStatusResetExhausted] != 1 {
		t.Fatalf("counts = %+v, %v", counts, err)
	}
}

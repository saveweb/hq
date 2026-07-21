package postgres_test

import (
	"context"
	"crypto/sha256"
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
	avatar := "https://avatars.example/admin"
	admin, err := store.UpsertGitHubAdmin(ctx, tracker.GitHubIdentity{UserID: 42, Login: "hq-admin", AvatarURL: &avatar}, now)
	if err != nil || admin.ID != "gh_42" || admin.GitHubUserID == nil || *admin.GitHubUserID != 42 || !admin.HasRole(tracker.RoleAdmin) {
		t.Fatalf("GitHub admin = %+v, %v", admin, err)
	}
	sessionHash := sha256.Sum256([]byte("integration-session"))
	if err := store.CreateWebSession(ctx, admin.ID, sessionHash[:], now, now+3600); err != nil {
		t.Fatal(err)
	}
	authenticated, err := store.AuthenticateWebSession(ctx, sessionHash[:], now+1)
	if err != nil || authenticated.ID != admin.ID || authenticated.GitHubLogin != "hq-admin" {
		t.Fatalf("authenticated web session = %+v, %v", authenticated, err)
	}
	if _, err := store.AuthenticateWebSession(ctx, sessionHash[:], now+3600); err == nil {
		t.Fatal("expired web session authenticated")
	}
	if err := store.DeleteWebSession(ctx, sessionHash[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateWebSession(ctx, sessionHash[:], now+2); err == nil {
		t.Fatal("deleted web session authenticated")
	}
	if err := store.PutUserAndToken(ctx, tracker.User{ID: "queue-worker", Status: tracker.UserStatusActive, Roles: map[string]bool{tracker.RoleWorker: true}}, "queue-worker-token", now); err != nil {
		t.Fatal(err)
	}
	if err := store.PutUser(ctx, "managed-user", tracker.UserStatusActive, []string{tracker.RoleWorker}, now); err != nil {
		t.Fatal(err)
	}
	if err := store.RotateMachineToken(ctx, "managed-user", "managed-token", now); err != nil {
		t.Fatal(err)
	}
	if user, err := store.AuthenticateMachineToken(ctx, "managed-token"); err != nil || user.ID != "managed-user" {
		t.Fatalf("managed token authentication = %+v, %v", user, err)
	}
	users, err := store.ListUsers(ctx)
	if err != nil || len(users) < 3 {
		t.Fatalf("users = %+v, %v", users, err)
	}
	if err := store.RevokeMachineToken(ctx, "managed-user", now+1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateMachineToken(ctx, "managed-token"); err == nil {
		t.Fatal("revoked token authenticated")
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "queue-project", Status: tracker.ProjectStatusActive}, now); err != nil {
		t.Fatal(err)
	}
	projects, err := store.ListProjectSummaries(ctx)
	if err != nil || len(projects) != 1 || projects[0].ID != "queue-project" || projects[0].JobCounts[protocol.JobStatusTodo] != 0 {
		t.Fatalf("initial project summaries = %+v, %v", projects, err)
	}
	jobs := []protocol.JobSpecV1{{ID: "job-1", Value: "https://example.test/1", Type: protocol.JobTypeSeed, Via: nil, Attrs: map[string]any{"source": "test"}}, {ID: "job-2", Value: "https://example.test/2", Via: nil}}
	inserted, err := store.EnqueueProjectJobs(ctx, "queue-project", jobs, now)
	if err != nil || inserted != 2 {
		t.Fatalf("enqueue = %d, %v", inserted, err)
	}
	inserted, err = store.EnqueueProjectJobs(ctx, "queue-project", jobs, now+1)
	if err != nil || inserted != 0 {
		t.Fatalf("idempotent enqueue = %d, %v", inserted, err)
	}
	conflict := append([]protocol.JobSpecV1(nil), jobs[:1]...)
	conflict[0].Value = "https://example.test/different"
	if _, err := store.EnqueueProjectJobs(ctx, "queue-project", conflict, now+2); !tracker.IsCode(err, protocol.ErrorIdentityConflict) {
		t.Fatalf("identity conflict = %v", err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "unique-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeUniqueValue}, now); err != nil {
		t.Fatal(err)
	}
	valueJobs := []protocol.JobSpecV1{{Value: "same-value"}, {Value: "same-value"}}
	if inserted, err := store.EnqueueProjectJobs(ctx, "unique-project", valueJobs, now); err != nil || inserted != 1 {
		t.Fatalf("unique-value enqueue = %d, %v", inserted, err)
	}
	if _, err := store.EnqueueProjectJobs(ctx, "unique-project", []protocol.JobSpecV1{{ID: "not-allowed", Value: "other"}}, now); !tracker.IsCode(err, protocol.ErrorInvalidJob) {
		t.Fatalf("unique-value external ID = %v", err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "unique-project", Status: tracker.ProjectStatusDraining, IdentityMode: tracker.IdentityModeNone}, now+1); !tracker.IsCode(err, protocol.ErrorInvalidRequest) {
		t.Fatalf("changed identity mode = %v", err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "none-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone}, now); err != nil {
		t.Fatal(err)
	}
	if inserted, err := store.EnqueueProjectJobs(ctx, "none-project", valueJobs, now); err != nil || inserted != 2 {
		t.Fatalf("non-unique enqueue = %d, %v", inserted, err)
	}
	if err := store.DeleteProject(ctx, "none-project"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProjectSummary(ctx, "none-project"); !tracker.IsCode(err, protocol.ErrorNotFound) {
		t.Fatalf("deleted project = %v", err)
	}
	listedJobs, err := store.ListProjectJobs(ctx, "queue-project", "", 0, 1)
	if err != nil || len(listedJobs.Jobs) != 1 || listedJobs.NextAfterJobID == nil {
		t.Fatalf("listed jobs = %+v, %v", listedJobs, err)
	}
	if job, err := store.ProjectJob(ctx, "queue-project", listedJobs.Jobs[0].JobID); err != nil || job.Value == "" {
		t.Fatalf("job detail = %+v, %v", job, err)
	}

	claimed, err := store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-1", MaxJobs: 1, LeaseSeconds: 300}, now+3)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}
	item := claimed[0]
	receipt := protocol.WARCReceipt{ID: "receipt-1", Issuer: "https://warc.test", ObjectID: "warc/object-1", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 1234, AcceptedAt: now + 4, Signature: "test-signature"}
	completed, err := store.CompleteProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectCompleteRequest{WorkerID: "worker-process-1", Items: []protocol.ProjectCompleteItem{{JobID: item.JobID, AttemptID: item.AttemptID, Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}}, WARCReceipts: []protocol.WARCReceipt{receipt}}}}, now+5)
	if err != nil || len(completed.Results) != 1 || completed.Results[0].Status != protocol.ItemStatusApplied {
		t.Fatalf("complete = %+v, %v", completed, err)
	}
	replayed, err := store.CompleteProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectCompleteRequest{WorkerID: "worker-process-1", Items: []protocol.ProjectCompleteItem{{JobID: item.JobID, AttemptID: item.AttemptID, Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}}, WARCReceipts: []protocol.WARCReceipt{receipt}}}}, now+6)
	if err != nil || replayed.Results[0].Status != protocol.ItemStatusRejected || replayed.Results[0].Error.Code != protocol.ErrorStaleAttempt {
		t.Fatalf("replayed complete = %+v, %v", replayed, err)
	}

	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-1", MaxJobs: 1, LeaseSeconds: 300}, now+7)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("second claim = %+v, %v", claimed, err)
	}
	extended, err := store.ExtendProjectJobLeases(ctx, "queue-worker", "queue-project", protocol.ProjectExtendLeaseRequest{WorkerID: "worker-process-1", ExtendSeconds: 600, Items: []protocol.AttemptRef{{JobID: claimed[0].JobID, AttemptID: claimed[0].AttemptID}}}, now+8)
	if err != nil || extended.Results[0].Status != protocol.ItemStatusApplied || *extended.Results[0].LeaseExpiresAt != now+608 {
		t.Fatalf("extend lease = %+v, %v", extended, err)
	}
	failed, err := store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-1", Items: []protocol.FailItem{{JobID: claimed[0].JobID, AttemptID: claimed[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "retry", Details: protocol.Attrs{}}}}}, now+9)
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
	stale, err := store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-1", Items: []protocol.FailItem{{JobID: expiredAttempt.JobID, AttemptID: expiredAttempt.AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "stale", Details: protocol.Attrs{}}}}}, now+13)
	if err != nil || stale.Results[0].Status != protocol.ItemStatusRejected {
		t.Fatalf("stale expired attempt = %+v, %v", stale, err)
	}
	failed, err = store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-2", Items: []protocol.FailItem{{JobID: claimed[0].JobID, AttemptID: claimed[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "retry", Details: protocol.Attrs{}}}}}, now+14)
	if err != nil || *failed.Results[0].JobStatus != protocol.JobStatusTodo {
		t.Fatalf("third reset = %+v, %v", failed, err)
	}
	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-2", MaxJobs: 1, LeaseSeconds: 30}, now+15)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("final claim = %+v, %v", claimed, err)
	}
	failed, err = store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-2", Items: []protocol.FailItem{{JobID: claimed[0].JobID, AttemptID: claimed[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "exhaust", Details: protocol.Attrs{}}}}}, now+16)
	if err != nil || *failed.Results[0].JobStatus != protocol.JobStatusResetExhausted {
		t.Fatalf("reset exhaustion = %+v, %v", failed, err)
	}

	counts, err := store.ProjectJobCounts(ctx, "queue-project")
	if err != nil || counts[protocol.JobStatusDone] != 1 || counts[protocol.JobStatusResetExhausted] != 1 {
		t.Fatalf("counts = %+v, %v", counts, err)
	}
	project, err := store.ProjectSummary(ctx, "queue-project")
	if err != nil || project.JobCounts[protocol.JobStatusDone] != 1 || project.JobCounts[protocol.JobStatusResetExhausted] != 1 {
		t.Fatalf("final project summary = %+v, %v", project, err)
	}
	if err := store.RequeueProjectJob(ctx, "queue-project", claimed[0].JobID, now+17); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteProjectJob(ctx, "queue-project", claimed[0].JobID); err != nil {
		t.Fatal(err)
	}
}

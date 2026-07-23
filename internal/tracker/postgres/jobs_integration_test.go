package postgres_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/saveweb/hq/internal/tracker"
	"github.com/saveweb/hq/internal/tracker/postgres"
	"github.com/saveweb/hq/pkg/protocol"
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
	pending, err := store.UpsertGitHubPendingWorker(ctx, tracker.GitHubIdentity{UserID: 43, Login: "new-worker"}, now)
	if err != nil || pending.ID != "gh_43" || pending.Status != tracker.UserStatusPending || !pending.HasRole(tracker.RoleWorker) {
		t.Fatalf("GitHub pending worker = %+v, %v", pending, err)
	}
	if err := store.PutUser(ctx, pending.ID, tracker.UserStatusActive, []string{tracker.RoleWorker}, now+1); err != nil {
		t.Fatal(err)
	}
	pending, err = store.UpsertGitHubPendingWorker(ctx, tracker.GitHubIdentity{UserID: 43, Login: "renamed-worker"}, now+2)
	if err != nil || pending.Status != tracker.UserStatusActive || !pending.HasRole(tracker.RoleWorker) || pending.GitHubLogin != "renamed-worker" {
		t.Fatalf("returning GitHub worker = %+v, %v", pending, err)
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
	if token, active, err := store.MachineToken(ctx, "managed-user"); err != nil || !active || token != "managed-token" {
		t.Fatalf("managed token state = %q, %v, %v", token, active, err)
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
	if token, active, err := store.MachineToken(ctx, "managed-user"); err != nil || active || token != "" {
		t.Fatalf("revoked token state = %q, %v, %v", token, active, err)
	}
	if err := store.RotateMachineToken(ctx, pending.ID, "pending-worker-token", now+3); err != nil {
		t.Fatal(err)
	}
	pendingSessionHash := sha256.Sum256([]byte("pending-worker-session"))
	if err := store.CreateWebSession(ctx, pending.ID, pendingSessionHash[:], now+3, now+3600); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteUser(ctx, pending.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateMachineToken(ctx, "pending-worker-token"); err == nil {
		t.Fatal("deleted user's token authenticated")
	}
	if _, err := store.AuthenticateWebSession(ctx, pendingSessionHash[:], now+4); err == nil {
		t.Fatal("deleted user's web session authenticated")
	}
	if err := store.DeleteUser(ctx, pending.ID); !tracker.IsCode(err, protocol.ErrorNotFound) {
		t.Fatalf("delete missing user = %v", err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "queue-project", Status: tracker.ProjectStatusActive, ClientVersions: []string{"worker-v2", "worker-v1"}}, now); err != nil {
		t.Fatal(err)
	}
	projects, err := store.ListProjectSummaries(ctx)
	if err != nil || len(projects) != 1 || projects[0].ID != "queue-project" || projects[0].ClaimOrder != tracker.ClaimOrderFIFO || projects[0].MaxResets != 3 || projects[0].RecommendedLeaseSeconds != 300 || len(projects[0].ClientVersions) != 2 || projects[0].ClientVersions[0] != "worker-v1" || projects[0].JobCounts[protocol.JobStatusTodo] != 0 {
		t.Fatalf("initial project summaries = %+v, %v", projects, err)
	}
	if err := store.CheckProjectClientVersion(ctx, "queue-worker", "queue-project", "worker-v2"); err != nil {
		t.Fatalf("allowed client version = %v", err)
	}
	if err := store.CheckProjectClientVersion(ctx, "queue-worker", "queue-project", "worker-v0"); !tracker.IsCode(err, protocol.ErrorClientUpgrade) {
		t.Fatalf("obsolete client version = %v", err)
	}
	if err := store.CheckProjectClientVersion(ctx, "queue-worker", "queue-project", ""); !tracker.IsCode(err, protocol.ErrorClientUpgrade) {
		t.Fatalf("missing client version = %v", err)
	}
	if err := store.CheckProjectClientVersion(ctx, admin.ID, "queue-project", "worker-v2"); !tracker.IsCode(err, protocol.ErrorPermissionDenied) {
		t.Fatalf("non-worker client version check = %v", err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "invalid-versions", Status: tracker.ProjectStatusActive, ClientVersions: []string{"v1", "v1"}}, now); !tracker.IsCode(err, protocol.ErrorInvalidRequest) {
		t.Fatalf("duplicate client versions = %v", err)
	}
	invalidMaxResets := 1001
	if err := store.PutProject(ctx, tracker.Project{ID: "invalid-resets", Status: tracker.ProjectStatusActive, MaxResets: &invalidMaxResets}, now); !tracker.IsCode(err, protocol.ErrorInvalidRequest) {
		t.Fatalf("invalid max resets = %v", err)
	}
	invalidLease := int64(3601)
	if err := store.PutProject(ctx, tracker.Project{ID: "invalid-lease", Status: tracker.ProjectStatusActive, RecommendedLeaseSeconds: &invalidLease}, now); !tracker.IsCode(err, protocol.ErrorInvalidRequest) {
		t.Fatalf("invalid recommended lease = %v", err)
	}
	jobs := []protocol.AdminEnqueueJob{{ID: "job-1", Value: "https://example.test/1", Type: protocol.JobTypeSeed, Via: nil, Attrs: map[string]any{"source": "test"}}, {ID: "job-2", Value: "https://example.test/2", Via: nil}}
	inserted, err := store.EnqueueProjectJobs(ctx, "queue-project", jobs, now)
	if err != nil || inserted != 2 {
		t.Fatalf("enqueue = %d, %v", inserted, err)
	}
	inserted, err = store.EnqueueProjectJobs(ctx, "queue-project", jobs, now+1)
	if err != nil || inserted != 0 {
		t.Fatalf("idempotent enqueue = %d, %v", inserted, err)
	}
	conflict := append([]protocol.AdminEnqueueJob(nil), jobs[:1]...)
	conflict[0].Value = "https://example.test/different"
	if _, err := store.EnqueueProjectJobs(ctx, "queue-project", conflict, now+2); !tracker.IsCode(err, protocol.ErrorIdentityConflict) {
		t.Fatalf("identity conflict = %v", err)
	}
	duplicateBatch := []protocol.AdminEnqueueJob{{ID: "batch-duplicate", Value: "same"}, {ID: "batch-duplicate", Value: "same"}}
	if inserted, err := store.EnqueueProjectJobs(ctx, "queue-project", duplicateBatch, now+2); err != nil || inserted != 1 {
		t.Fatalf("same-batch duplicate enqueue = %d, %v", inserted, err)
	}
	keyOne, keyTwo := int32(1), int32(2)
	conflictingKeys := []protocol.AdminEnqueueJob{{ID: "key-conflict", Value: "same", RandomKey: &keyOne}, {ID: "key-conflict", Value: "same", RandomKey: &keyTwo}}
	if _, err := store.EnqueueProjectJobs(ctx, "queue-project", conflictingKeys, now+2); !tracker.IsCode(err, protocol.ErrorIdentityConflict) {
		t.Fatalf("same-batch random key conflict = %v", err)
	}
	atomicJob := protocol.AdminEnqueueJob{ID: "atomic-new", Value: "new"}
	atomicConflict := []protocol.AdminEnqueueJob{atomicJob, {ID: "job-1", Value: "conflicting"}}
	if _, err := store.EnqueueProjectJobs(ctx, "queue-project", atomicConflict, now+2); !tracker.IsCode(err, protocol.ErrorIdentityConflict) {
		t.Fatalf("atomic identity conflict = %v", err)
	}
	if inserted, err := store.EnqueueProjectJobs(ctx, "queue-project", []protocol.AdminEnqueueJob{atomicJob}, now+2); err != nil || inserted != 1 {
		t.Fatalf("conflicting bulk enqueue did not roll back = %d, %v", inserted, err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "large-enqueue-project", Status: tracker.ProjectStatusActive}, now); err != nil {
		t.Fatal(err)
	}
	largeBatch := make([]protocol.AdminEnqueueJob, 300)
	for index := range largeBatch {
		largeBatch[index] = protocol.AdminEnqueueJob{ID: "large-" + strconv.Itoa(index), Value: strconv.Itoa(index)}
	}
	if inserted, err := store.EnqueueProjectJobs(ctx, "large-enqueue-project", largeBatch, now); err != nil || inserted != int64(len(largeBatch)) {
		t.Fatalf("large enqueue = %d, %v", inserted, err)
	}
	if err := store.DeleteProject(ctx, "large-enqueue-project"); err != nil {
		t.Fatal(err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "unique-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeUniqueValue}, now); err != nil {
		t.Fatal(err)
	}
	valueJobs := []protocol.AdminEnqueueJob{{Value: "same-value"}, {Value: "same-value"}}
	if inserted, err := store.EnqueueProjectJobs(ctx, "unique-project", valueJobs, now); err != nil || inserted != 1 {
		t.Fatalf("unique-value enqueue = %d, %v", inserted, err)
	}
	if _, err := store.EnqueueProjectJobs(ctx, "unique-project", []protocol.AdminEnqueueJob{{Value: "same-value", Attrs: map[string]any{"changed": true}}}, now); !tracker.IsCode(err, protocol.ErrorIdentityConflict) {
		t.Fatalf("unique-value identity conflict = %v", err)
	}
	if _, err := store.EnqueueProjectJobs(ctx, "unique-project", []protocol.AdminEnqueueJob{{ID: "not-allowed", Value: "other"}}, now); !tracker.IsCode(err, protocol.ErrorInvalidJob) {
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

	claimed, err := store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-1", MaxJobs: 1, LeaseSeconds: 300, PolicyVersion: 1}, (now+3)*1_000_000_000)
	if err != nil || len(claimed.Jobs) != 1 || claimed.Jobs[0].Value != "https://example.test/1" {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}
	item := claimed.Jobs[0]
	receipt := protocol.ArtifactReceipt{ID: "receipt-1", Issuer: "https://artifacts.test", ObjectID: "artifacts/object-1", Checksum: "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 1234, AcceptedAt: now + 4}
	completed, err := store.CompleteProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectCompleteRequest{WorkerID: "worker-process-1", Items: []protocol.ProjectCompleteItem{{JobID: item.JobID, AttemptID: item.AttemptID, Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}}, ArtifactReceipts: []protocol.ArtifactReceipt{receipt}}}}, now+5)
	if err != nil || len(completed.Results) != 1 || completed.Results[0].Status != protocol.ItemStatusApplied {
		t.Fatalf("complete = %+v, %v", completed, err)
	}
	workerUserID, found, err := store.WorkerUserID(ctx, "worker-process-1")
	if err != nil || !found || workerUserID != "queue-worker" {
		t.Fatalf("worker user = %q, %t, %v", workerUserID, found, err)
	}
	workers, err := store.ListWorkers(ctx, "worker-process-1", 200)
	if err != nil || len(workers) != 1 || workers[0].UserID != "queue-worker" {
		t.Fatalf("workers = %+v, %v", workers, err)
	}
	if err := store.DeleteWorker(ctx, "worker-process-1"); err != nil {
		t.Fatal(err)
	}
	if workerUserID, found, err = store.WorkerUserID(ctx, "worker-process-1"); err != nil || found || workerUserID != "" {
		t.Fatalf("deleted worker user = %q, %t, %v", workerUserID, found, err)
	}
	replayed, err := store.CompleteProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectCompleteRequest{WorkerID: "worker-process-1", Items: []protocol.ProjectCompleteItem{{JobID: item.JobID, AttemptID: item.AttemptID, Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}}, ArtifactReceipts: []protocol.ArtifactReceipt{receipt}}}}, now+6)
	if err != nil || replayed.Results[0].Status != protocol.ItemStatusRejected || replayed.Results[0].Error.Code != protocol.ErrorStaleAttempt {
		t.Fatalf("replayed complete = %+v, %v", replayed, err)
	}

	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-1", MaxJobs: 1, LeaseSeconds: 300, PolicyVersion: 1}, (now+7)*1_000_000_000)
	if err != nil || len(claimed.Jobs) != 1 {
		t.Fatalf("second claim = %+v, %v", claimed, err)
	}
	extended, err := store.ExtendProjectJobLeases(ctx, "queue-worker", "queue-project", protocol.ProjectExtendLeaseRequest{WorkerID: "worker-process-1", ExtendSeconds: 600, Items: []protocol.AttemptRef{{JobID: claimed.Jobs[0].JobID, AttemptID: claimed.Jobs[0].AttemptID}}}, now+8)
	if err != nil || extended.Results[0].Status != protocol.ItemStatusApplied || *extended.Results[0].LeaseExpiresAt != now+608 {
		t.Fatalf("extend lease = %+v, %v", extended, err)
	}
	failed, err := store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-1", Items: []protocol.FailItem{{JobID: claimed.Jobs[0].JobID, AttemptID: claimed.Jobs[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "retry", Details: protocol.Attrs{}}}}}, now+9)
	if err != nil || failed.Results[0].Status != protocol.ItemStatusApplied || *failed.Results[0].JobStatus != protocol.JobStatusTodo {
		t.Fatalf("retryable fail = %+v, %v", failed, err)
	}

	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-1", MaxJobs: 1, LeaseSeconds: 1, AcceptTypes: []string{protocol.JobTypeSeed}, PolicyVersion: 1}, (now+10)*1_000_000_000)
	if err != nil || len(claimed.Jobs) != 1 || claimed.Jobs[0].Type != protocol.JobTypeSeed {
		t.Fatalf("normalized claim = %+v, %v", claimed, err)
	}
	expiredAttempt := claimed.Jobs[0]
	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-2", MaxJobs: 1, LeaseSeconds: 30, PolicyVersion: 1}, (now+12)*1_000_000_000)
	if err != nil || len(claimed.Jobs) != 1 || claimed.Jobs[0].AttemptID == expiredAttempt.AttemptID {
		t.Fatalf("reclaim expired lease = %+v, %v", claimed, err)
	}
	stale, err := store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-1", Items: []protocol.FailItem{{JobID: expiredAttempt.JobID, AttemptID: expiredAttempt.AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "stale", Details: protocol.Attrs{}}}}}, now+13)
	if err != nil || stale.Results[0].Status != protocol.ItemStatusRejected {
		t.Fatalf("stale expired attempt = %+v, %v", stale, err)
	}
	failed, err = store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-2", Items: []protocol.FailItem{{JobID: claimed.Jobs[0].JobID, AttemptID: claimed.Jobs[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "retry", Details: protocol.Attrs{}}}}}, now+14)
	if err != nil || *failed.Results[0].JobStatus != protocol.JobStatusTodo {
		t.Fatalf("third reset = %+v, %v", failed, err)
	}
	claimed, err = store.ClaimProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-2", MaxJobs: 1, LeaseSeconds: 30, PolicyVersion: 1}, (now+15)*1_000_000_000)
	if err != nil || len(claimed.Jobs) != 1 {
		t.Fatalf("final claim = %+v, %v", claimed, err)
	}
	failed, err = store.FailProjectJobs(ctx, "queue-worker", "queue-project", protocol.ProjectFailRequest{WorkerID: "worker-process-2", Items: []protocol.FailItem{{JobID: claimed.Jobs[0].JobID, AttemptID: claimed.Jobs[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "exhaust", Details: protocol.Attrs{}}}}}, now+16)
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
	if err := store.RequeueProjectJob(ctx, "queue-project", claimed.Jobs[0].JobID, now+17); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteProjectJob(ctx, "queue-project", claimed.Jobs[0].JobID); err != nil {
		t.Fatal(err)
	}

	zeroResets := 0
	if err := store.PutProject(ctx, tracker.Project{ID: "no-reset-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone, MaxResets: &zeroResets}, now+17); err != nil {
		t.Fatal(err)
	}
	if inserted, err := store.EnqueueProjectJobs(ctx, "no-reset-project", []protocol.AdminEnqueueJob{{Value: "retryable"}, {Value: "expires"}}, now+17); err != nil || inserted != 2 {
		t.Fatalf("no-reset enqueue = %d, %v", inserted, err)
	}
	noResetClaim, err := store.ClaimProjectJobs(ctx, "queue-worker", "no-reset-project", protocol.ProjectClaimRequest{WorkerID: "no-reset-worker", MaxJobs: 1, LeaseSeconds: 30, PolicyVersion: 1}, (now+18)*1_000_000_000)
	if err != nil || len(noResetClaim.Jobs) != 1 {
		t.Fatalf("no-reset claim = %+v, %v", noResetClaim, err)
	}
	noResetFailure, err := store.FailProjectJobs(ctx, "queue-worker", "no-reset-project", protocol.ProjectFailRequest{WorkerID: "no-reset-worker", Items: []protocol.FailItem{{JobID: noResetClaim.Jobs[0].JobID, AttemptID: noResetClaim.Jobs[0].AttemptID, Retryable: true, Error: protocol.ExecutionError{Code: "temporary", Message: "retry", Details: protocol.Attrs{}}}}}, now+19)
	if err != nil || *noResetFailure.Results[0].JobStatus != protocol.JobStatusResetExhausted {
		t.Fatalf("disabled retry = %+v, %v", noResetFailure, err)
	}
	noResetClaim, err = store.ClaimProjectJobs(ctx, "queue-worker", "no-reset-project", protocol.ProjectClaimRequest{WorkerID: "no-reset-worker", MaxJobs: 1, LeaseSeconds: 1, PolicyVersion: 1}, (now+20)*1_000_000_000)
	if err != nil || len(noResetClaim.Jobs) != 1 {
		t.Fatalf("expiring no-reset claim = %+v, %v", noResetClaim, err)
	}
	noResetClaim, err = store.ClaimProjectJobs(ctx, "queue-worker", "no-reset-project", protocol.ProjectClaimRequest{WorkerID: "no-reset-worker", MaxJobs: 1, LeaseSeconds: 30, PolicyVersion: 1}, (now+22)*1_000_000_000)
	if err != nil || len(noResetClaim.Jobs) != 0 {
		t.Fatalf("disabled lease reset = %+v, %v", noResetClaim, err)
	}
	noResetCounts, err := store.ProjectJobCounts(ctx, "no-reset-project")
	if err != nil || noResetCounts[protocol.JobStatusResetExhausted] != 2 {
		t.Fatalf("no-reset counts = %+v, %v", noResetCounts, err)
	}
	oneReset := 1
	recommendedLease := int64(120)
	if err := store.PutProject(ctx, tracker.Project{ID: "no-reset-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone, MaxResets: &oneReset, RecommendedLeaseSeconds: &recommendedLease}, now+23); err != nil {
		t.Fatal(err)
	}
	updatedResetProject, err := store.ProjectSummary(ctx, "no-reset-project")
	if err != nil || updatedResetProject.MaxResets != 1 || updatedResetProject.RecommendedLeaseSeconds != 120 || updatedResetProject.PolicyVersion != 2 {
		t.Fatalf("updated reset policy = %+v, %v", updatedResetProject, err)
	}

	low, high := int32(-100), int32(100)
	if err := store.PutProject(ctx, tracker.Project{ID: "random-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone, ClaimOrder: tracker.ClaimOrderRandom}, now+18); err != nil {
		t.Fatal(err)
	}
	randomJobs := []protocol.AdminEnqueueJob{{Value: "high", RandomKey: &high}, {Value: "low", RandomKey: &low}}
	if inserted, err := store.EnqueueProjectJobs(ctx, "random-project", randomJobs, now+19); err != nil || inserted != 2 {
		t.Fatalf("random enqueue = %d, %v", inserted, err)
	}
	randomClaim, err := store.ClaimProjectJobs(ctx, "queue-worker", "random-project", protocol.ProjectClaimRequest{WorkerID: "worker-process-1", MaxJobs: 2, LeaseSeconds: 300, PolicyVersion: 1}, (now+20)*1_000_000_000)
	if err != nil || len(randomClaim.Jobs) != 2 || randomClaim.Jobs[0].Value != "low" || randomClaim.Jobs[1].Value != "high" {
		t.Fatalf("random claim = %+v, %v", randomClaim, err)
	}
	storedLow, err := store.ProjectJob(ctx, "random-project", randomClaim.Jobs[0].JobID)
	if err != nil || storedLow.RandomKey != low {
		t.Fatalf("stored random key = %+v, %v", storedLow, err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "random-project", Status: tracker.ProjectStatusActive, ClaimOrder: tracker.ClaimOrderFIFO}, now+21); err != nil {
		t.Fatalf("switch claim order = %v", err)
	}
	randomProject, err := store.ProjectSummary(ctx, "random-project")
	if err != nil || randomProject.ClaimOrder != tracker.ClaimOrderFIFO {
		t.Fatalf("switched project = %+v, %v", randomProject, err)
	}

	dispatchQPS, workerClaimQPS := 2.5, 0.1
	if err := store.PutProject(ctx, tracker.Project{
		ID: "limited-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone,
		DispatchQPS: &dispatchQPS, WorkerClaimQPS: &workerClaimQPS, MaxJobsPerClaim: 8,
	}, now+22); err != nil {
		t.Fatal(err)
	}
	limitedPolicy, err := store.ProjectPolicy(ctx, "queue-worker", "limited-project")
	if err != nil || limitedPolicy.DispatchQPS == nil || *limitedPolicy.DispatchQPS != dispatchQPS ||
		limitedPolicy.WorkerClaimQPS == nil || *limitedPolicy.WorkerClaimQPS != workerClaimQPS ||
		limitedPolicy.MaxJobsPerClaim != 8 || limitedPolicy.RecommendedLeaseSeconds != 300 || limitedPolicy.PolicyVersion != 1 {
		t.Fatalf("limited policy = %+v, %v", limitedPolicy, err)
	}
	limitedJobs := []protocol.AdminEnqueueJob{{Value: "limited-1"}, {Value: "limited-2"}, {Value: "limited-3"}}
	if inserted, err := store.EnqueueProjectJobs(ctx, "limited-project", limitedJobs, now+23); err != nil || inserted != 3 {
		t.Fatalf("limited enqueue = %d, %v", inserted, err)
	}
	limitedNow := (now + 24) * 1_000_000_000
	limitedClaim, err := store.ClaimProjectJobs(ctx, "queue-worker", "limited-project", protocol.ProjectClaimRequest{
		WorkerID: "limited-worker", MaxJobs: 8, LeaseSeconds: 300, PolicyVersion: 1,
	}, limitedNow)
	if err != nil || len(limitedClaim.Jobs) != 1 || limitedClaim.RetryAfterMS != 400 {
		t.Fatalf("first limited claim = %+v, %v", limitedClaim, err)
	}
	_, err = store.ClaimProjectJobs(ctx, "queue-worker", "limited-project", protocol.ProjectClaimRequest{
		WorkerID: "limited-worker", MaxJobs: 8, LeaseSeconds: 300, PolicyVersion: 1,
	}, limitedNow+100_000_000)
	var limitedError *tracker.Error
	if !errors.As(err, &limitedError) || limitedError.Code != protocol.ErrorProjectRateLimited || limitedError.RetryAfter != 300 {
		t.Fatalf("limited retry = %#v", err)
	}
	limitedClaim, err = store.ClaimProjectJobs(ctx, "queue-worker", "limited-project", protocol.ProjectClaimRequest{
		WorkerID: "limited-worker", MaxJobs: 8, LeaseSeconds: 300, PolicyVersion: 1,
	}, limitedNow+400_000_000)
	if err != nil || len(limitedClaim.Jobs) != 1 {
		t.Fatalf("second limited claim = %+v, %v", limitedClaim, err)
	}
	limitedClaim, err = store.ClaimProjectJobs(ctx, "queue-worker", "limited-project", protocol.ProjectClaimRequest{
		WorkerID: "limited-worker", MaxJobs: 8, LeaseSeconds: 300, PolicyVersion: 1,
	}, limitedNow+800_000_000)
	if err != nil || len(limitedClaim.Jobs) != 1 {
		t.Fatalf("third limited claim = %+v, %v", limitedClaim, err)
	}
	limitedClaim, err = store.ClaimProjectJobs(ctx, "queue-worker", "limited-project", protocol.ProjectClaimRequest{
		WorkerID: "limited-worker", MaxJobs: 8, LeaseSeconds: 300, PolicyVersion: 1,
	}, limitedNow+800_000_000)
	if err != nil || len(limitedClaim.Jobs) != 0 {
		t.Fatalf("empty limited claim = %+v, %v", limitedClaim, err)
	}
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(ctx)
	var claimRequests, dispatchedJobs int64
	if err := connection.QueryRow(ctx, `
		SELECT claim_requests,jobs_dispatched
		FROM tracker_worker_claim_buckets
		WHERE project_id='limited-project' AND worker_id='limited-worker'
	`).Scan(&claimRequests, &dispatchedJobs); err != nil || claimRequests != 5 || dispatchedJobs != 3 {
		t.Fatalf("worker claim bucket = requests %d jobs %d, %v", claimRequests, dispatchedJobs, err)
	}
	var unlimitedBuckets int
	if err := connection.QueryRow(ctx, `SELECT count(*) FROM tracker_worker_claim_buckets WHERE project_id='queue-project'`).Scan(&unlimitedBuckets); err != nil || unlimitedBuckets != 0 {
		t.Fatalf("unlimited project buckets = %d, %v", unlimitedBuckets, err)
	}
	if err := store.PutProject(ctx, tracker.Project{
		ID: "limited-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone,
		DispatchQPS: &dispatchQPS, WorkerClaimQPS: &workerClaimQPS, MaxJobsPerClaim: 8,
	}, now+25); err != nil {
		t.Fatal(err)
	}
	unchangedPolicy, err := store.ProjectPolicy(ctx, "queue-worker", "limited-project")
	if err != nil || unchangedPolicy.PolicyVersion != 1 {
		t.Fatalf("unchanged policy = %+v, %v", unchangedPolicy, err)
	}
	changedWorkerClaimQPS := 0.2
	if err := store.PutProject(ctx, tracker.Project{
		ID: "limited-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone,
		DispatchQPS: &dispatchQPS, WorkerClaimQPS: &changedWorkerClaimQPS, MaxJobsPerClaim: 8,
	}, now+26); err != nil {
		t.Fatal(err)
	}
	changedPolicy, err := store.ProjectPolicy(ctx, "queue-worker", "limited-project")
	if err != nil || changedPolicy.PolicyVersion != 2 {
		t.Fatalf("changed policy = %+v, %v", changedPolicy, err)
	}
	if err := store.PutProject(ctx, tracker.Project{
		ID: "limited-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone,
		DispatchQPS: &dispatchQPS, WorkerClaimQPS: &changedWorkerClaimQPS, MaxJobsPerClaim: 8,
		ClientVersions: []string{"worker-v3"},
	}, now+27); err != nil {
		t.Fatal(err)
	}
	changedVersionsPolicy, err := store.ProjectPolicy(ctx, "queue-worker", "limited-project")
	if err != nil || changedVersionsPolicy.PolicyVersion != 3 {
		t.Fatalf("changed client versions policy = %+v, %v", changedVersionsPolicy, err)
	}

	highQPS := 2000.0
	if err := store.PutProject(ctx, tracker.Project{
		ID: "high-qps-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone,
		DispatchQPS: &highQPS, MaxJobsPerClaim: 256,
	}, now+25); err != nil {
		t.Fatal(err)
	}
	highJobs := make([]protocol.AdminEnqueueJob, 256)
	for index := range highJobs {
		highJobs[index] = protocol.AdminEnqueueJob{Value: "high-qps-" + strconv.Itoa(index)}
	}
	if inserted, err := store.EnqueueProjectJobs(ctx, "high-qps-project", highJobs, now+26); err != nil || inserted != 256 {
		t.Fatalf("high QPS enqueue = %d, %v", inserted, err)
	}
	highClaim, err := store.ClaimProjectJobs(ctx, "queue-worker", "high-qps-project", protocol.ProjectClaimRequest{
		WorkerID: "high-qps-worker", MaxJobs: 256, LeaseSeconds: 300, PolicyVersion: 1,
	}, (now+27)*1_000_000_000)
	if err != nil || len(highClaim.Jobs) != 200 {
		t.Fatalf("high QPS claim = %d jobs, %v", len(highClaim.Jobs), err)
	}

	extremeQPS := 1e300
	if err := store.PutProject(ctx, tracker.Project{
		ID: "extreme-qps-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone,
		DispatchQPS: &extremeQPS, MaxJobsPerClaim: 256,
	}, now+27); err != nil {
		t.Fatal(err)
	}
	extremeJobs := make([]protocol.AdminEnqueueJob, 257)
	for index := range extremeJobs {
		extremeJobs[index] = protocol.AdminEnqueueJob{Value: "extreme-qps-" + strconv.Itoa(index)}
	}
	if inserted, err := store.EnqueueProjectJobs(ctx, "extreme-qps-project", extremeJobs, now+28); err != nil || inserted != 257 {
		t.Fatalf("extreme QPS enqueue = %d, %v", inserted, err)
	}
	extremeNow := (now + 29) * 1_000_000_000
	extremeClaim, err := store.ClaimProjectJobs(ctx, "queue-worker", "extreme-qps-project", protocol.ProjectClaimRequest{
		WorkerID: "extreme-qps-worker", MaxJobs: 256, LeaseSeconds: 300, PolicyVersion: 1,
	}, extremeNow)
	if err != nil || len(extremeClaim.Jobs) != 256 {
		t.Fatalf("extreme QPS claim = %d jobs, %v", len(extremeClaim.Jobs), err)
	}
	_, err = store.ClaimProjectJobs(ctx, "queue-worker", "extreme-qps-project", protocol.ProjectClaimRequest{
		WorkerID: "extreme-qps-worker", MaxJobs: 256, LeaseSeconds: 300, PolicyVersion: 1,
	}, extremeNow)
	if !errors.As(err, &limitedError) || limitedError.Code != protocol.ErrorProjectRateLimited {
		t.Fatalf("extreme QPS same-time retry = %#v", err)
	}

	if err := store.PutProject(ctx, tracker.Project{
		ID: "batch-cap-project", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeNone,
		MaxJobsPerClaim: 1,
	}, now+28); err != nil {
		t.Fatal(err)
	}
	if inserted, err := store.EnqueueProjectJobs(ctx, "batch-cap-project", limitedJobs, now+29); err != nil || inserted != 3 {
		t.Fatalf("batch cap enqueue = %d, %v", inserted, err)
	}
	cappedClaim, err := store.ClaimProjectJobs(ctx, "queue-worker", "batch-cap-project", protocol.ProjectClaimRequest{
		WorkerID: "batch-cap-worker", MaxJobs: 256, LeaseSeconds: 300, PolicyVersion: 1,
	}, (now+30)*1_000_000_000)
	if err != nil || len(cappedClaim.Jobs) != 1 {
		t.Fatalf("batch cap claim = %+v, %v", cappedClaim, err)
	}
}

func BenchmarkPostgresEnqueueBatches(b *testing.B) {
	databaseURL := os.Getenv("HQ_TEST_POSTGRES_URL")
	if databaseURL == "" {
		b.Skip("HQ_TEST_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	store, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		b.Fatal(err)
	}

	const batchSize = 1000
	repeated := make([]protocol.AdminEnqueueJob, batchSize)
	for index := range repeated {
		repeated[index] = protocol.AdminEnqueueJob{ID: "repeat-" + strconv.Itoa(index), Value: strconv.Itoa(index)}
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "benchmark-repeat", Status: tracker.ProjectStatusActive}, 1); err != nil {
		b.Fatal(err)
	}
	if _, err := store.EnqueueProjectJobs(ctx, "benchmark-repeat", repeated, 1); err != nil {
		b.Fatal(err)
	}
	b.Run("repeat-1000", func(b *testing.B) {
		b.ReportMetric(float64(batchSize), "jobs/op")
		for range b.N {
			if inserted, err := store.EnqueueProjectJobs(ctx, "benchmark-repeat", repeated, 2); err != nil || inserted != 0 {
				b.Fatalf("enqueue = %d, %v", inserted, err)
			}
		}
	})

	if err := store.PutProject(ctx, tracker.Project{ID: "benchmark-fresh", Status: tracker.ProjectStatusActive}, 1); err != nil {
		b.Fatal(err)
	}
	var freshRun int
	b.Run("fresh-1000", func(b *testing.B) {
		freshRun++
		b.ReportMetric(float64(batchSize), "jobs/op")
		jobs := make([]protocol.AdminEnqueueJob, batchSize)
		for iteration := range b.N {
			b.StopTimer()
			prefix := strconv.Itoa(freshRun) + "-" + strconv.Itoa(iteration) + "-"
			for index := range jobs {
				value := prefix + strconv.Itoa(index)
				jobs[index] = protocol.AdminEnqueueJob{ID: value, Value: value}
			}
			b.StartTimer()
			if inserted, err := store.EnqueueProjectJobs(ctx, "benchmark-fresh", jobs, 2); err != nil || inserted != batchSize {
				b.Fatalf("enqueue = %d, %v", inserted, err)
			}
		}
	})

	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		b.Fatal(err)
	}
	defer connection.Close(ctx)
	if err := store.PutProject(ctx, tracker.Project{ID: "benchmark-legacy-repeat", Status: tracker.ProjectStatusActive}, 1); err != nil {
		b.Fatal(err)
	}
	if _, err := legacyEnqueueExternalID(ctx, connection, "benchmark-legacy-repeat", repeated, 1); err != nil {
		b.Fatal(err)
	}
	b.Run("legacy-repeat-1000", func(b *testing.B) {
		b.ReportMetric(float64(batchSize), "jobs/op")
		for range b.N {
			if inserted, err := legacyEnqueueExternalID(ctx, connection, "benchmark-legacy-repeat", repeated, 2); err != nil || inserted != 0 {
				b.Fatalf("enqueue = %d, %v", inserted, err)
			}
		}
	})

	if err := store.PutProject(ctx, tracker.Project{ID: "benchmark-legacy-fresh", Status: tracker.ProjectStatusActive}, 1); err != nil {
		b.Fatal(err)
	}
	var legacyFreshRun int
	b.Run("legacy-fresh-1000", func(b *testing.B) {
		legacyFreshRun++
		b.ReportMetric(float64(batchSize), "jobs/op")
		jobs := make([]protocol.AdminEnqueueJob, batchSize)
		for iteration := range b.N {
			b.StopTimer()
			prefix := strconv.Itoa(legacyFreshRun) + "-" + strconv.Itoa(iteration) + "-"
			for index := range jobs {
				value := prefix + strconv.Itoa(index)
				jobs[index] = protocol.AdminEnqueueJob{ID: value, Value: value}
			}
			b.StartTimer()
			if inserted, err := legacyEnqueueExternalID(ctx, connection, "benchmark-legacy-fresh", jobs, 2); err != nil || inserted != batchSize {
				b.Fatalf("enqueue = %d, %v", inserted, err)
			}
		}
	})
}

func legacyEnqueueExternalID(ctx context.Context, connection *pgx.Conn, projectID string, jobs []protocol.AdminEnqueueJob, now int64) (int64, error) {
	var inserted int64
	err := pgx.BeginFunc(ctx, connection, func(tx pgx.Tx) error {
		var status, identityMode string
		if err := tx.QueryRow(ctx, `SELECT status,identity_mode FROM tracker_projects WHERE id=$1`, projectID).Scan(&status, &identityMode); err != nil {
			return err
		}
		for _, job := range jobs {
			tag, err := tx.Exec(ctx, `
				INSERT INTO tracker_jobs(project_id,external_id,value,spec,random_key,status,created_at,updated_at)
				VALUES ($1,$2,$3,'{}',0,'todo',$4,$4)
				ON CONFLICT (project_id,external_id) WHERE external_id IS NOT NULL DO NOTHING
			`, projectID, job.ID, job.Value, now)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				var value string
				var spec []byte
				if err := tx.QueryRow(ctx, `SELECT value,spec FROM tracker_jobs WHERE project_id=$1 AND external_id=$2`, projectID, job.ID).Scan(&value, &spec); err != nil {
					return err
				}
			}
			inserted += tag.RowsAffected()
		}
		return nil
	})
	return inserted, err
}

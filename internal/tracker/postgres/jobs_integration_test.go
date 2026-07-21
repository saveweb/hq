package postgres_test

import (
	"context"
	"crypto/sha256"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"

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
	duplicateBatch := []protocol.JobSpecV1{{ID: "batch-duplicate", Value: "same"}, {ID: "batch-duplicate", Value: "same"}}
	if inserted, err := store.EnqueueProjectJobs(ctx, "queue-project", duplicateBatch, now+2); err != nil || inserted != 1 {
		t.Fatalf("same-batch duplicate enqueue = %d, %v", inserted, err)
	}
	atomicJob := protocol.JobSpecV1{ID: "atomic-new", Value: "new"}
	atomicConflict := []protocol.JobSpecV1{atomicJob, {ID: "job-1", Value: "conflicting"}}
	if _, err := store.EnqueueProjectJobs(ctx, "queue-project", atomicConflict, now+2); !tracker.IsCode(err, protocol.ErrorIdentityConflict) {
		t.Fatalf("atomic identity conflict = %v", err)
	}
	if inserted, err := store.EnqueueProjectJobs(ctx, "queue-project", []protocol.JobSpecV1{atomicJob}, now+2); err != nil || inserted != 1 {
		t.Fatalf("conflicting bulk enqueue did not roll back = %d, %v", inserted, err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "large-enqueue-project", Status: tracker.ProjectStatusActive}, now); err != nil {
		t.Fatal(err)
	}
	largeBatch := make([]protocol.JobSpecV1, 300)
	for index := range largeBatch {
		largeBatch[index] = protocol.JobSpecV1{ID: "large-" + strconv.Itoa(index), Value: strconv.Itoa(index)}
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
	valueJobs := []protocol.JobSpecV1{{Value: "same-value"}, {Value: "same-value"}}
	if inserted, err := store.EnqueueProjectJobs(ctx, "unique-project", valueJobs, now); err != nil || inserted != 1 {
		t.Fatalf("unique-value enqueue = %d, %v", inserted, err)
	}
	if _, err := store.EnqueueProjectJobs(ctx, "unique-project", []protocol.JobSpecV1{{Value: "same-value", Attrs: map[string]any{"changed": true}}}, now); !tracker.IsCode(err, protocol.ErrorIdentityConflict) {
		t.Fatalf("unique-value identity conflict = %v", err)
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
	repeated := make([]protocol.JobSpecV1, batchSize)
	for index := range repeated {
		repeated[index] = protocol.JobSpecV1{ID: "repeat-" + strconv.Itoa(index), Value: strconv.Itoa(index)}
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
		jobs := make([]protocol.JobSpecV1, batchSize)
		for iteration := range b.N {
			b.StopTimer()
			prefix := strconv.Itoa(freshRun) + "-" + strconv.Itoa(iteration) + "-"
			for index := range jobs {
				value := prefix + strconv.Itoa(index)
				jobs[index] = protocol.JobSpecV1{ID: value, Value: value}
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
		jobs := make([]protocol.JobSpecV1, batchSize)
		for iteration := range b.N {
			b.StopTimer()
			prefix := strconv.Itoa(legacyFreshRun) + "-" + strconv.Itoa(iteration) + "-"
			for index := range jobs {
				value := prefix + strconv.Itoa(index)
				jobs[index] = protocol.JobSpecV1{ID: value, Value: value}
			}
			b.StartTimer()
			if inserted, err := legacyEnqueueExternalID(ctx, connection, "benchmark-legacy-fresh", jobs, 2); err != nil || inserted != batchSize {
				b.Fatalf("enqueue = %d, %v", inserted, err)
			}
		}
	})
}

func legacyEnqueueExternalID(ctx context.Context, connection *pgx.Conn, projectID string, jobs []protocol.JobSpecV1, now int64) (int64, error) {
	var inserted int64
	err := pgx.BeginFunc(ctx, connection, func(tx pgx.Tx) error {
		var status, identityMode string
		if err := tx.QueryRow(ctx, `SELECT status,identity_mode FROM tracker_projects WHERE id=$1`, projectID).Scan(&status, &identityMode); err != nil {
			return err
		}
		for _, job := range jobs {
			tag, err := tx.Exec(ctx, `
				INSERT INTO tracker_jobs(project_id,external_id,value,spec,status,created_at,updated_at)
				VALUES ($1,$2,$3,'{}','todo',$4,$4)
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

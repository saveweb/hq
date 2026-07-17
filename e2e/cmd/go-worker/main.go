package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
	"git.saveweb.org/saveweb/hq/sdk/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	phase := flag.String("phase", "", "initial, verify, takeover, recover, source, or checkpoint-recovery")
	projectID := flag.String("project-id", "project-e2e", "explicit project identifier")
	trackerURL := flag.String("tracker-url", "", "tracker base URL")
	tokenFile := flag.String("machine-token-file", "", "worker machine token file")
	readyFile := flag.String("ready-file", "", "takeover readiness file")
	continueFile := flag.String("continue-file", "", "takeover continuation file")
	flag.Parse()
	if flag.NArg() != 0 || *phase == "" || *trackerURL == "" || *tokenFile == "" {
		return fmt.Errorf("go-worker: phase, tracker-url, and machine-token-file are required")
	}
	raw, err := os.ReadFile(*tokenFile)
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(raw))
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	session, err := worker.OpenSession(ctx, worker.Config{
		TrackerURL: *trackerURL, MachineToken: token, AgentID: "worker-go-e2e",
		AgentName: "Go E2E worker", AgentVersion: "e2e", AllowHTTPTracker: true,
		RequestTimeout: 10 * time.Second,
	}, *projectID, protocol.Attrs{"sdk": "go", "phase": *phase})
	if err != nil {
		return err
	}
	defer session.Close()
	switch *phase {
	case "initial":
		return initial(ctx, session)
	case "verify":
		return verify(ctx, session)
	case "takeover":
		if *readyFile == "" || *continueFile == "" {
			return fmt.Errorf("go-worker: takeover requires ready-file and continue-file")
		}
		return takeover(ctx, session, *readyFile, *continueFile)
	case "recover":
		return recoverJob(ctx, session)
	case "source":
		return consumeSource(ctx, session)
	case "checkpoint-recovery":
		return consumeCheckpointRecovery(ctx, session)
	default:
		return fmt.Errorf("go-worker: invalid phase %q", *phase)
	}
}

func consumeCheckpointRecovery(ctx context.Context, session *worker.Session) error {
	batch, err := session.Claim(ctx, 1, 60, []string{protocol.JobTypeSeed})
	if err != nil {
		return err
	}
	jobs := batch.Jobs()
	if len(jobs) != 1 || jobs[0].ID != "checkpoint-recovery" || batch.Route().Generation != 2 {
		return fmt.Errorf("go-worker: unexpected checkpoint recovery claim: route=%+v jobs=%+v", batch.Route(), jobs)
	}
	result, err := batch.Complete(ctx, []protocol.CompleteItem{{
		JobID: jobs[0].ID, AttemptID: jobs[0].AttemptID,
		Outcome:        protocol.Outcome{Kind: "success", Meta: protocol.Attrs{"checkpoint_recovery": true}},
		DiscoveredJobs: []protocol.JobSpecV1{},
	}})
	if err != nil {
		return err
	}
	if len(result.Results) != 1 || result.Results[0].Status != protocol.ItemStatusApplied {
		return fmt.Errorf("go-worker: checkpoint recovery completion was not applied: %+v", result)
	}
	return nil
}

func consumeSource(ctx context.Context, session *worker.Session) error {
	batch, err := session.Claim(ctx, 10, 60, []string{protocol.JobTypeSeed})
	if err != nil {
		return err
	}
	jobs := batch.Jobs()
	if len(jobs) != 2 || batch.Route().Generation != 1 {
		return fmt.Errorf("go-worker: unexpected source jobs: route=%+v jobs=%+v", batch.Route(), jobs)
	}
	items := make([]protocol.CompleteItem, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, protocol.CompleteItem{
			JobID: job.ID, AttemptID: job.AttemptID,
			Outcome:        protocol.Outcome{Kind: "success", Meta: protocol.Attrs{"source_e2e": true}},
			DiscoveredJobs: []protocol.JobSpecV1{},
		})
	}
	result, err := batch.Complete(ctx, items)
	if err != nil {
		return err
	}
	if len(result.Results) != len(jobs) {
		return fmt.Errorf("go-worker: incomplete source results: %+v", result)
	}
	for _, item := range result.Results {
		if item.Status != protocol.ItemStatusApplied {
			return fmt.Errorf("go-worker: source completion was not applied: %+v", result)
		}
	}
	return nil
}

func initial(ctx context.Context, session *worker.Session) error {
	batch, err := session.Claim(ctx, 1, 60, []string{protocol.JobTypeSeed})
	if err != nil {
		return err
	}
	jobs := batch.Jobs()
	if len(jobs) != 1 || jobs[0].ID != "a-go" {
		return fmt.Errorf("go-worker: unexpected initial jobs: %+v", jobs)
	}
	extended, err := batch.ExtendLease(ctx, 30, []protocol.AttemptRef{{JobID: jobs[0].ID, AttemptID: jobs[0].AttemptID}})
	if err != nil {
		return fmt.Errorf("go-worker: extend lease: %w", err)
	}
	if len(extended.Results) != 1 || extended.Results[0].Status != protocol.ItemStatusApplied {
		return fmt.Errorf("go-worker: unexpected extend result: %+v", extended)
	}
	completed, err := batch.Complete(ctx, []protocol.CompleteItem{{
		JobID: jobs[0].ID, AttemptID: jobs[0].AttemptID,
		Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{"worker": "go"}},
		DiscoveredJobs: []protocol.JobSpecV1{{
			ID: "c-go-failed", URL: "https://example.test/go-discovered", Type: protocol.JobTypeSeed,
			Attrs: map[string]any{"discovered_by": "go"},
		}},
	}})
	if err != nil {
		return fmt.Errorf("go-worker: complete: %w", err)
	}
	if len(completed.Results) != 1 || completed.Results[0].Status != protocol.ItemStatusApplied {
		return fmt.Errorf("go-worker: unexpected complete result: %+v", completed)
	}
	return nil
}

func verify(ctx context.Context, session *worker.Session) error {
	batch, err := session.Claim(ctx, 10, 60, []string{protocol.JobTypeSeed})
	if err != nil {
		return err
	}
	jobs := batch.Jobs()
	if len(jobs) != 2 || jobs[0].ID != "c-go-failed" || jobs[1].ID != "d-python-done" {
		return fmt.Errorf("go-worker: unexpected verification jobs: %+v", jobs)
	}
	failed, err := batch.Fail(ctx, []protocol.FailItem{{
		JobID: jobs[0].ID, AttemptID: jobs[0].AttemptID, Retryable: false,
		Error: protocol.ExecutionError{Code: "e2e_failure", Message: "expected E2E failure", Details: protocol.Attrs{}},
	}})
	if err != nil {
		return fmt.Errorf("go-worker: fail: %w", err)
	}
	if len(failed.Results) != 1 || failed.Results[0].Status != protocol.ItemStatusApplied {
		return fmt.Errorf("go-worker: unexpected fail result: %+v", failed)
	}
	completed, err := batch.Complete(ctx, []protocol.CompleteItem{{
		JobID: jobs[1].ID, AttemptID: jobs[1].AttemptID,
		Outcome:        protocol.Outcome{Kind: "success", Meta: protocol.Attrs{"worker": "go-verifier"}},
		DiscoveredJobs: []protocol.JobSpecV1{},
	}})
	if err != nil {
		return fmt.Errorf("go-worker: verification complete: %w", err)
	}
	if len(completed.Results) != 1 || completed.Results[0].Status != protocol.ItemStatusApplied {
		return fmt.Errorf("go-worker: unexpected verification result: %+v", completed)
	}
	return nil
}

func takeover(ctx context.Context, session *worker.Session, readyFile, continueFile string) error {
	batch, err := session.Claim(ctx, 1, 60, []string{protocol.JobTypeAsset})
	if err != nil {
		return err
	}
	jobs := batch.Jobs()
	if len(jobs) != 1 || jobs[0].ID != "e-takeover" || batch.Route().Generation != 1 {
		return fmt.Errorf("go-worker: unexpected takeover claim: route=%+v jobs=%+v", batch.Route(), jobs)
	}
	if err := os.WriteFile(readyFile, []byte("ready\n"), 0o600); err != nil {
		return err
	}
	for {
		if _, err := os.Stat(continueFile); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	_, err = batch.Complete(ctx, []protocol.CompleteItem{{
		JobID: jobs[0].ID, AttemptID: jobs[0].AttemptID,
		Outcome:        protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}},
		DiscoveredJobs: []protocol.JobSpecV1{},
	}})
	if !errors.Is(err, worker.ErrRouteRetired) {
		return fmt.Errorf("go-worker: takeover complete error = %v, want route retired", err)
	}
	return nil
}

func recoverJob(ctx context.Context, session *worker.Session) error {
	batch, err := session.Claim(ctx, 1, 60, []string{protocol.JobTypeAsset})
	if err != nil {
		return err
	}
	jobs := batch.Jobs()
	if len(jobs) != 1 || jobs[0].ID != "e-takeover" || batch.Route().Generation != 2 {
		return fmt.Errorf("go-worker: unexpected recovered claim: route=%+v jobs=%+v", batch.Route(), jobs)
	}
	result, err := batch.Complete(ctx, []protocol.CompleteItem{{
		JobID: jobs[0].ID, AttemptID: jobs[0].AttemptID,
		Outcome:        protocol.Outcome{Kind: "success", Meta: protocol.Attrs{"generation": 2}},
		DiscoveredJobs: []protocol.JobSpecV1{},
	}})
	if err != nil {
		return fmt.Errorf("go-worker: recovered complete: %w", err)
	}
	if len(result.Results) != 1 || result.Results[0].Status != protocol.ItemStatusApplied {
		return fmt.Errorf("go-worker: unexpected recovered result: %+v", result)
	}
	return nil
}

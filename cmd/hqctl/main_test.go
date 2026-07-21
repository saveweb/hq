package main

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestEnqueueValuesUsesBoundedBatchesAndProjectIdentity(t *testing.T) {
	client := &fakeAdminAPI{}
	result, err := enqueueBatches(t.Context(), client, protocol.AdminProjectSummary{
		ID: "demo", IdentityMode: tracker.IdentityModeExternalID,
	}, strings.NewReader("1\n2\n\n3\r\n4\n5\n"), "values", "", 2, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.batchSizes, []int{2, 2, 1}) {
		t.Fatalf("batch sizes = %v", client.batchSizes)
	}
	if len(client.jobs) != 5 || client.jobs[0].Value != "1" || client.jobs[2].Value != "3" || client.jobs[0].ID != protocol.DefaultJobID(protocol.JobTypeSeed, "1") {
		t.Fatalf("jobs = %+v", client.jobs)
	}
	if result.ProjectID != "demo" || result.IdentityMode != tracker.IdentityModeExternalID || result.Submitted != 5 || result.Inserted != 5 || result.Batches != 3 {
		t.Fatalf("result = %+v", result)
	}
}

func TestEnqueueJSONLRejectsIDForUniqueValueProject(t *testing.T) {
	client := &fakeAdminAPI{}
	result, err := enqueueBatches(t.Context(), client, protocol.AdminProjectSummary{
		ID: "demo", IdentityMode: tracker.IdentityModeUniqueValue,
	}, strings.NewReader("{\"id\":\"source-1\",\"value\":\"1\"}\n"), "jsonl", "", 256, 100, nil)
	if err == nil || !strings.Contains(err.Error(), "rejects job id") || result.Submitted != 0 || client.calls != 0 {
		t.Fatalf("result=%+v calls=%d error=%v", result, client.calls, err)
	}
}

type fakeAdminAPI struct {
	calls      int
	batchSizes []int
	jobs       []protocol.JobSpecV1
}

func (f *fakeAdminAPI) AdminProject(context.Context, string) (protocol.AdminProjectSummary, error) {
	panic("not used")
}

func (f *fakeAdminAPI) EnqueueAdminProjectJobs(_ context.Context, _ string, jobs []protocol.JobSpecV1) (protocol.AdminEnqueueJobsResponse, error) {
	f.calls++
	f.batchSizes = append(f.batchSizes, len(jobs))
	f.jobs = append(f.jobs, jobs...)
	return protocol.AdminEnqueueJobsResponse{ProjectID: "demo", Submitted: len(jobs), Inserted: int64(len(jobs))}, nil
}

func (f *fakeAdminAPI) EnqueueAdminProjectSource(context.Context, string, io.Reader) (protocol.AdminEnqueueSourceResponse, error) {
	panic("not used")
}

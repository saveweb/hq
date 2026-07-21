package trackerclient

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestClaimProjectJobsRequest(t *testing.T) {
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.String() != "https://hq.test/api/v1/projects/project-1/jobs/claim" {
			t.Fatalf("request = %s %s", request.Method, request.URL)
		}
		if request.Header.Get("Authorization") != "Bearer machine-token" {
			t.Fatalf("authorization = %q", request.Header.Get("Authorization"))
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), `"worker_id":"worker-1"`) {
			t.Fatalf("body = %s", body)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type":  []string{"application/json"},
				"Cache-Control": []string{"no-store"},
			},
			Body: io.NopCloser(strings.NewReader(`{"project_id":"project-1","jobs":[],"retry_after_ms":1000}`)),
		}, nil
	})
	client, err := New(Config{
		BaseURL: "https://hq.test", MachineToken: "machine-token", WorkerID: "worker-1",
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.ClaimProjectJobs(context.Background(), "project-1", protocol.ProjectClaimRequest{WorkerID: "worker-1", MaxJobs: 1, LeaseSeconds: 30})
	if err != nil || response.ProjectID != "project-1" || response.Jobs == nil {
		t.Fatalf("response = %+v, %v", response, err)
	}
}

func TestProjectJobsErrorEnvelope(t *testing.T) {
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusConflict,
			Header: http.Header{
				"Content-Type":  []string{"application/json"},
				"Cache-Control": []string{"no-store"},
			},
			Body: io.NopCloser(strings.NewReader(`{"error":{"code":"project_not_active","message":"project is not active","retryable":false,"retry_after_ms":0,"details":{}}}`)),
		}, nil
	})
	client, err := New(Config{BaseURL: "https://hq.test", MachineToken: "token", WorkerID: "worker-1", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ClaimProjectJobs(context.Background(), "project-1", protocol.ProjectClaimRequest{})
	clientError, ok := err.(*Error)
	if !ok || clientError.Status != http.StatusConflict || clientError.API.Code != protocol.ErrorProjectNotActive {
		t.Fatalf("error = %#v", err)
	}
}

func TestAdminProjectAndEnqueueSourceRequests(t *testing.T) {
	var calls int
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		if request.Header.Get("Authorization") != "Bearer admin-token" {
			t.Fatalf("authorization = %q", request.Header.Get("Authorization"))
		}
		body := ""
		switch calls {
		case 1:
			if request.Method != http.MethodGet || request.URL.String() != "https://hq.test/api/v1/admin/projects/demo" {
				t.Fatalf("project request = %s %s", request.Method, request.URL)
			}
			body = `{"id":"demo","status":"active","identity_mode":"external_id","claim_order":"fifo","job_counts":{},"created_at":1,"updated_at":1}`
		case 2:
			if request.Method != http.MethodPost || request.URL.String() != "https://hq.test/api/v1/admin/projects/demo/jobs" || request.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("jobs request = %s %s headers=%v", request.Method, request.URL, request.Header)
			}
			raw, err := io.ReadAll(request.Body)
			if err != nil || !strings.Contains(string(raw), `"value":"one"`) || !strings.Contains(string(raw), `"random_key":-7`) {
				t.Fatalf("jobs body = %q error=%v", raw, err)
			}
			body = `{"project_id":"demo","submitted":1,"inserted":1}`
		case 3:
			if request.Method != http.MethodPost || request.URL.String() != "https://hq.test/api/v1/admin/projects/demo/source" || request.Header.Get("Content-Type") != "application/zstd" {
				t.Fatalf("source request = %s %s headers=%v", request.Method, request.URL, request.Header)
			}
			raw, err := io.ReadAll(request.Body)
			if err != nil || string(raw) != "packed-source" {
				t.Fatalf("source body = %q error=%v", raw, err)
			}
			body = `{"project_id":"demo","jobs":10,"inserted":9,"uncompressed_bytes":123}`
		default:
			t.Fatalf("unexpected call %d", calls)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "Cache-Control": []string{"no-store"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})
	client, err := New(Config{BaseURL: "https://hq.test", MachineToken: "admin-token", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	project, err := client.AdminProject(context.Background(), "demo")
	if err != nil || project.ID != "demo" || project.IdentityMode != "external_id" {
		t.Fatalf("project=%+v error=%v", project, err)
	}
	randomKey := int32(-7)
	jobsResult, err := client.EnqueueAdminProjectJobs(context.Background(), "demo", []protocol.AdminEnqueueJob{{Value: "one", RandomKey: &randomKey}})
	if err != nil || jobsResult.Submitted != 1 || jobsResult.Inserted != 1 {
		t.Fatalf("jobs result=%+v error=%v", jobsResult, err)
	}
	result, err := client.EnqueueAdminProjectSource(context.Background(), "demo", strings.NewReader("packed-source"))
	if err != nil || result.Jobs != 10 || result.Inserted != 9 || result.UncompressedBytes != 123 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

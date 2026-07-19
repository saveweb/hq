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

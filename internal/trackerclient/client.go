// Package trackerclient is the shared HTTP client for the Project Queue API.
package trackerclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const maxResponseBytes = int64(8 << 20)

type Config struct {
	BaseURL, MachineToken, WorkerID string
	AllowHTTP                       bool
	RequestTimeout                  time.Duration
	HTTPClient                      *http.Client
}
type Client struct {
	baseURL, machineToken string
	httpClient            *http.Client
}
type Error struct {
	Status int
	API    protocol.APIError
}

func (e *Error) Error() string {
	return fmt.Sprintf("tracker HTTP %d: %s: %s", e.Status, e.API.Code, e.API.Message)
}

func New(config Config) (*Client, error) {
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") ||
		(parsed.Scheme != "https" && !(config.AllowHTTP && parsed.Scheme == "http")) {
		return nil, fmt.Errorf("tracker client: invalid tracker URL")
	}
	if config.MachineToken == "" || len(config.MachineToken) > 1024 {
		return nil, fmt.Errorf("tracker client: invalid machine credentials")
	}
	client := config.HTTPClient
	if client == nil {
		timeout := config.RequestTimeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{baseURL: strings.TrimSuffix(config.BaseURL, "/"), machineToken: config.MachineToken, httpClient: &copyClient}, nil
}

func (c *Client) AdminProject(ctx context.Context, projectID string) (protocol.AdminProjectSummary, error) {
	var result protocol.AdminProjectSummary
	err := c.do(ctx, http.MethodGet, adminProjectPath(projectID), "", nil, &result)
	return result, err
}

func (c *Client) EnqueueAdminProjectJobs(ctx context.Context, projectID string, jobs []protocol.JobSpecV1) (protocol.AdminEnqueueJobsResponse, error) {
	encoded, err := json.Marshal(protocol.AdminEnqueueJobsRequest{Jobs: jobs})
	if err != nil {
		return protocol.AdminEnqueueJobsResponse{}, err
	}
	var result protocol.AdminEnqueueJobsResponse
	err = c.do(ctx, http.MethodPost, adminProjectPath(projectID)+"/jobs", "application/json", bytes.NewReader(encoded), &result)
	return result, err
}

func (c *Client) EnqueueAdminProjectSource(ctx context.Context, projectID string, source io.Reader) (protocol.AdminEnqueueSourceResponse, error) {
	var result protocol.AdminEnqueueSourceResponse
	err := c.do(ctx, http.MethodPost, adminProjectPath(projectID)+"/source", "application/zstd", source, &result)
	return result, err
}

func adminProjectPath(projectID string) string {
	return "/api/v1/admin/projects/" + url.PathEscape(projectID)
}

func (c *Client) ClaimProjectJobs(ctx context.Context, projectID string, request protocol.ProjectClaimRequest) (protocol.ProjectClaimResponse, error) {
	var result protocol.ProjectClaimResponse
	err := c.doJSON(ctx, projectJobsPath(projectID)+"/claim", request, &result)
	return result, err
}
func (c *Client) CompleteProjectJobs(ctx context.Context, projectID string, request protocol.ProjectCompleteRequest) (protocol.BatchResultResponse, error) {
	var result protocol.BatchResultResponse
	err := c.doJSON(ctx, projectJobsPath(projectID)+"/complete", request, &result)
	return result, err
}
func (c *Client) FailProjectJobs(ctx context.Context, projectID string, request protocol.ProjectFailRequest) (protocol.BatchResultResponse, error) {
	var result protocol.BatchResultResponse
	err := c.doJSON(ctx, projectJobsPath(projectID)+"/fail", request, &result)
	return result, err
}
func (c *Client) ExtendProjectJobLeases(ctx context.Context, projectID string, request protocol.ProjectExtendLeaseRequest) (protocol.BatchResultResponse, error) {
	var result protocol.BatchResultResponse
	err := c.doJSON(ctx, projectJobsPath(projectID)+"/extend-lease", request, &result)
	return result, err
}
func projectJobsPath(projectID string) string {
	return "/api/v1/projects/" + url.PathEscape(projectID) + "/jobs"
}

func (c *Client) doJSON(ctx context.Context, endpoint string, input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, endpoint, "application/json", bytes.NewReader(encoded), output)
}

func (c *Client) do(ctx context.Context, method, endpoint, contentType string, body io.Reader, output any) error {
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+endpoint, body)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+c.machineToken)
	request.Header.Set("Accept", "application/json")
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if statter, ok := body.(interface{ Stat() (fs.FileInfo, error) }); ok {
		if info, statErr := statter.Stat(); statErr == nil && info.Mode().IsRegular() {
			request.ContentLength = info.Size()
		}
	}
	request.Header.Set("Cache-Control", "no-store, no-cache, max-age=0")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("tracker client: request: %w", err)
	}
	defer response.Body.Close()
	if !strings.Contains(strings.ToLower(response.Header.Get("Cache-Control")), "no-store") {
		return fmt.Errorf("tracker client: response is cacheable")
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return fmt.Errorf("tracker client: response is not JSON")
	}
	limited := io.LimitReader(response.Body, maxResponseBytes+1)
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope protocol.ErrorEnvelope
		if err := decoder.Decode(&envelope); err != nil {
			return err
		}
		return &Error{Status: response.StatusCode, API: envelope.Error}
	}
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("tracker client: trailing JSON")
	}
	return nil
}

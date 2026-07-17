// Package trackerclient is the shared HTTP client for shard and worker agents.
package trackerclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const maxResponseBytes = int64(8 << 20)

type Config struct {
	BaseURL      string
	MachineToken string
	AgentID      string
	AllowHTTP    bool
	HTTPClient   *http.Client
}

type Client struct {
	baseURL      string
	machineToken string
	agentID      string
	httpClient   *http.Client
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
		(parsed.Scheme != "https" && !(config.AllowHTTP && parsed.Scheme == "http")) {
		return nil, fmt.Errorf("tracker client: invalid tracker URL")
	}
	if config.MachineToken == "" || len(config.MachineToken) > 1024 || config.AgentID == "" {
		return nil, fmt.Errorf("tracker client: invalid machine credentials")
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{
		baseURL: strings.TrimSuffix(config.BaseURL, "/"), machineToken: config.MachineToken,
		agentID: config.AgentID, httpClient: &copyClient,
	}, nil
}

func (c *Client) UpsertAgent(ctx context.Context, request protocol.AgentUpsertRequest) (protocol.AgentResponse, error) {
	var result protocol.AgentResponse
	err := c.doJSON(ctx, http.MethodPut, "/api/v1/agents/"+url.PathEscape(c.agentID), request, &result)
	return result, err
}

func (c *Client) HeartbeatAgent(ctx context.Context, request protocol.AgentHeartbeatRequest) (protocol.AgentHeartbeatResponse, error) {
	var result protocol.AgentHeartbeatResponse
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/agents/"+url.PathEscape(c.agentID)+"/heartbeat", request, &result)
	return result, err
}

func (c *Client) ReportShardLoad(
	ctx context.Context,
	projectID, shardID string,
	request protocol.ShardLoadResultRequest,
) (protocol.ShardLoadResultResponse, error) {
	var result protocol.ShardLoadResultResponse
	err := c.doJSON(ctx, http.MethodPost,
		"/api/v1/shards/"+url.PathEscape(projectID)+"/"+url.PathEscape(shardID)+"/load-result",
		request, &result)
	return result, err
}

func (c *Client) CreateSession(ctx context.Context, request protocol.CreateSessionRequest) (protocol.SessionResponse, error) {
	var result protocol.SessionResponse
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/worker/sessions", request, &result)
	return result, err
}

func (c *Client) HeartbeatSession(ctx context.Context, sessionID string) (protocol.SessionResponse, error) {
	var result protocol.SessionResponse
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/worker/sessions/"+url.PathEscape(sessionID)+"/heartbeat", nil, &result)
	return result, err
}

func (c *Client) GetAssignment(ctx context.Context, request protocol.GetAssignmentRequest) (protocol.GetAssignmentResponse, error) {
	var result protocol.GetAssignmentResponse
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/worker/assignments", request, &result)
	return result, err
}

func (c *Client) doJSON(ctx context.Context, method, endpoint string, input, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("tracker client: encode request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+endpoint, body)
	if err != nil {
		return fmt.Errorf("tracker client: create request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.machineToken)
	request.Header.Set("X-Saveweb-Agent-ID", c.agentID)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store, no-cache, max-age=0")
	request.Header.Set("Pragma", "no-cache")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("tracker client: request: %w", err)
	}
	defer response.Body.Close()
	if err := validateCacheHeaders(response.Header); err != nil {
		return err
	}
	mediaType, _, mediaError := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaError != nil || mediaType != "application/json" {
		return fmt.Errorf("tracker client: response content type is not application/json")
	}
	limited := io.LimitReader(response.Body, maxResponseBytes+1)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope protocol.ErrorEnvelope
		if err := decodeStrict(limited, &envelope); err != nil {
			return fmt.Errorf("tracker client: HTTP %d with invalid error response: %w", response.StatusCode, err)
		}
		return &Error{Status: response.StatusCode, API: envelope.Error}
	}
	if output == nil {
		_, err := io.Copy(io.Discard, limited)
		return err
	}
	if err := decodeStrict(limited, output); err != nil {
		return fmt.Errorf("tracker client: decode response: %w", err)
	}
	return nil
}

func validateCacheHeaders(headers http.Header) error {
	if !strings.Contains(strings.ToLower(headers.Get("Cache-Control")), "no-store") {
		return &Error{Status: 0, API: protocol.APIError{
			Code: protocol.ErrorCacheMisconfigured, Message: "tracker response is missing Cache-Control: no-store",
		}}
	}
	switch strings.ToUpper(strings.TrimSpace(headers.Get("CF-Cache-Status"))) {
	case "", "DYNAMIC", "BYPASS":
		return nil
	default:
		return &Error{Status: 0, API: protocol.APIError{
			Code: protocol.ErrorCacheMisconfigured, Message: "tracker response may have been served from cache",
		}}
	}
}

func decodeStrict(reader io.Reader, output any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected exactly one JSON object")
	}
	return nil
}

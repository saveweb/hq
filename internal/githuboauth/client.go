// Package githuboauth implements the narrow GitHub OAuth surface used by the
// tracker contributor portal. Access tokens are returned only long enough to
// fetch the stable numeric GitHub identity.
package githuboauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"git.saveweb.org/saveweb/hq/internal/tracker"
)

const maxResponseBytes = int64(64 << 10)

type Config struct {
	ClientID       string
	ClientSecret   string
	RedirectURL    string
	AuthorizeURL   string
	AccessTokenURL string
	APIBaseURL     string
	HTTPClient     *http.Client
}

type Client struct {
	clientID       string
	clientSecret   string
	redirectURL    string
	authorizeURL   string
	accessTokenURL string
	apiBaseURL     string
	http           *http.Client
}

func New(config Config) (*Client, error) {
	if config.ClientID == "" || config.ClientSecret == "" || config.RedirectURL == "" {
		return nil, fmt.Errorf("github oauth: client ID, secret, and redirect URL are required")
	}
	if config.AuthorizeURL == "" {
		config.AuthorizeURL = "https://github.com/login/oauth/authorize"
	}
	if config.AccessTokenURL == "" {
		config.AccessTokenURL = "https://github.com/login/oauth/access_token"
	}
	if config.APIBaseURL == "" {
		config.APIBaseURL = "https://api.github.com"
	}
	for _, value := range []string{config.RedirectURL, config.AuthorizeURL, config.AccessTokenURL, config.APIBaseURL} {
		parsed, err := url.Parse(value)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
			return nil, fmt.Errorf("github oauth: invalid URL")
		}
	}
	if config.HTTPClient == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		config.HTTPClient = &http.Client{
			Timeout:   15 * time.Second,
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &Client{
		clientID: config.ClientID, clientSecret: config.ClientSecret,
		redirectURL: config.RedirectURL, authorizeURL: config.AuthorizeURL,
		accessTokenURL: config.AccessTokenURL, apiBaseURL: strings.TrimSuffix(config.APIBaseURL, "/"),
		http: config.HTTPClient,
	}, nil
}

func (c *Client) AuthorizationURL(state, codeChallenge string) (string, error) {
	if len(state) < 32 || len(codeChallenge) != 43 {
		return "", fmt.Errorf("github oauth: invalid state or PKCE challenge")
	}
	parsed, err := url.Parse(c.authorizeURL)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("client_id", c.clientID)
	query.Set("redirect_uri", c.redirectURL)
	query.Set("state", state)
	query.Set("code_challenge", codeChallenge)
	query.Set("code_challenge_method", "S256")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (c *Client) Exchange(ctx context.Context, code, codeVerifier string) (string, error) {
	if code == "" || len(code) > 1024 || len(codeVerifier) != 43 {
		return "", fmt.Errorf("github oauth: invalid authorization callback")
	}
	form := url.Values{
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
		"code":          {code},
		"redirect_uri":  {c.redirectURL},
		"code_verifier": {codeVerifier},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.accessTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("User-Agent", "SavewebHQ-tracker")
	var response struct {
		AccessToken      string `json:"access_token"`
		TokenType        string `json:"token_type"`
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	if err := c.doJSON(request, &response); err != nil {
		return "", err
	}
	if response.Error != "" {
		return "", fmt.Errorf("github oauth: token exchange rejected: %s", response.Error)
	}
	if response.AccessToken == "" || len(response.AccessToken) > 4096 || !strings.EqualFold(response.TokenType, "bearer") {
		return "", fmt.Errorf("github oauth: invalid access token response")
	}
	return response.AccessToken, nil
}

func (c *Client) User(ctx context.Context, accessToken string) (tracker.GitHubIdentity, error) {
	if accessToken == "" || len(accessToken) > 4096 {
		return tracker.GitHubIdentity{}, fmt.Errorf("github oauth: invalid access token")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiBaseURL+"/user", nil)
	if err != nil {
		return tracker.GitHubIdentity{}, err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+accessToken)
	request.Header.Set("User-Agent", "SavewebHQ-tracker")
	var response struct {
		ID        int64  `json:"id"`
		Login     string `json:"login"`
		AvatarURL string `json:"avatar_url"`
	}
	if err := c.doJSON(request, &response); err != nil {
		return tracker.GitHubIdentity{}, err
	}
	if response.ID < 1 || response.Login == "" || len(response.Login) > 255 || len(response.AvatarURL) > 2048 {
		return tracker.GitHubIdentity{}, fmt.Errorf("github oauth: invalid user response")
	}
	var avatar *string
	if response.AvatarURL != "" {
		avatar = &response.AvatarURL
	}
	return tracker.GitHubIdentity{UserID: response.ID, Login: response.Login, AvatarURL: avatar}, nil
}

func (c *Client) doJSON(request *http.Request, target any) error {
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("github oauth: request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBytes))
		return fmt.Errorf("github oauth: upstream HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxResponseBytes+1))
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("github oauth: decode response: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("github oauth: response must contain one JSON object")
	}
	return nil
}

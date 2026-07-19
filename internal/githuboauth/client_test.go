package githuboauth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestOAuthFlowAndTeamMembership(t *testing.T) {
	t.Helper()
	var api *httptest.Server
	api = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/token":
			if request.Method != http.MethodPost {
				t.Fatalf("token method = %s", request.Method)
			}
			if err := request.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if request.Form.Get("client_id") != "client" || request.Form.Get("client_secret") != "secret" || request.Form.Get("code") != "code" || request.Form.Get("code_verifier") != strings.Repeat("v", 43) {
				t.Fatalf("unexpected token form: %v", request.Form)
			}
			_ = json.NewEncoder(response).Encode(map[string]string{"access_token": "token", "token_type": "bearer"})
		case "/user":
			assertGitHubAPIRequest(t, request)
			_ = json.NewEncoder(response).Encode(map[string]any{"id": 42, "login": "octocat", "avatar_url": "https://avatars.example/42"})
		case "/orgs/saveweb/teams/core/memberships/octocat":
			assertGitHubAPIRequest(t, request)
			_ = json.NewEncoder(response).Encode(map[string]string{"state": "active"})
		case "/orgs/saveweb/teams/core/memberships/outsider":
			assertGitHubAPIRequest(t, request)
			response.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(response, `{}`)
		default:
			http.NotFound(response, request)
		}
	}))
	defer api.Close()

	client, err := New(Config{
		ClientID: "client", ClientSecret: "secret", RedirectURL: "https://hq.example/auth/github/callback",
		AuthorizeURL: api.URL + "/authorize", AccessTokenURL: api.URL + "/token", APIBaseURL: api.URL, HTTPClient: api.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	authorize, err := client.AuthorizationURL(strings.Repeat("s", 43), strings.Repeat("c", 43))
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(authorize)
	if query := parsed.Query(); query.Get("scope") != "read:org" || query.Get("state") != strings.Repeat("s", 43) || query.Get("code_challenge_method") != "S256" || query.Get("redirect_uri") != "https://hq.example/auth/github/callback" {
		t.Fatalf("unexpected authorize query: %v", query)
	}
	token, err := client.Exchange(context.Background(), "code", strings.Repeat("v", 43))
	if err != nil || token != "token" {
		t.Fatalf("Exchange = %q, %v", token, err)
	}
	identity, err := client.User(context.Background(), token)
	if err != nil || identity.UserID != 42 || identity.Login != "octocat" {
		t.Fatalf("User = %+v, %v", identity, err)
	}
	member, err := client.TeamMembership(context.Background(), token, "saveweb", "core", "octocat")
	if err != nil || !member {
		t.Fatalf("membership = %v, %v", member, err)
	}
	member, err = client.TeamMembership(context.Background(), token, "saveweb", "core", "outsider")
	if err != nil || member {
		t.Fatalf("outsider membership = %v, %v", member, err)
	}
}

func assertGitHubAPIRequest(t *testing.T, request *http.Request) {
	t.Helper()
	if request.Header.Get("Authorization") != "Bearer token" || request.Header.Get("X-GitHub-Api-Version") != githubAPIVersion || request.Header.Get("Accept") != "application/vnd.github+json" {
		t.Fatalf("unexpected GitHub API headers: %v", request.Header)
	}
}

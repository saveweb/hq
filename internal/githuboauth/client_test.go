package githuboauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestOAuthExchangeAndStableUserIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/token":
			if err := request.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if request.Form.Get("code_verifier") != "abcdefghijklmnopqrstuvwxyzABCDEFGH123456789" {
				t.Fatalf("verifier = %q", request.Form.Get("code_verifier"))
			}
			_ = json.NewEncoder(response).Encode(map[string]string{
				"access_token": "github-token", "token_type": "bearer",
			})
		case "/user":
			if request.Header.Get("Authorization") != "Bearer github-token" {
				t.Fatalf("authorization = %q", request.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(response).Encode(map[string]any{
				"id": int64(12345), "login": "contributor", "avatar_url": "https://avatar.test/u",
			})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client, err := New(Config{
		ClientID: "client", ClientSecret: "secret", RedirectURL: "https://tracker.test/auth/github/callback",
		AuthorizeURL: server.URL + "/authorize", AccessTokenURL: server.URL + "/token", APIBaseURL: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	authorize, err := client.AuthorizationURL("0123456789abcdef0123456789abcdef", "0123456789abcdef0123456789abcdef01234567890")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(authorize)
	if parsed.Query().Get("code_challenge_method") != "S256" || parsed.Query().Get("scope") != "" {
		t.Fatalf("authorization URL = %s", authorize)
	}
	verifier := "abcdefghijklmnopqrstuvwxyzABCDEFGH123456789"
	if len(verifier) != 43 {
		t.Fatalf("test verifier has length %d", len(verifier))
	}
	token, err := client.Exchange(context.Background(), "code", verifier)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := client.User(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if identity.UserID != 12345 || identity.Login != "contributor" {
		t.Fatalf("identity = %+v", identity)
	}
}

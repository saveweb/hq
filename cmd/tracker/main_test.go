package main

import (
	"encoding/base64"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/signingkey"
	"git.saveweb.org/saveweb/hq/internal/tracker"
)

func TestKeygenCommandCreatesLoadableExclusiveKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tracker.json")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run([]string{"keygen", "--out", path, "--key-id", "key-1"}, logger); err != nil {
		t.Fatal(err)
	}
	if _, err := signingkey.Load(path); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"keygen", "--out", path, "--key-id", "key-2"}, logger); err == nil {
		t.Fatal("keygen overwrote an existing key")
	}
}

func TestWebKeygenCreatesPrivateExclusiveSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "web.secret")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run([]string{"web-keygen", "--out", path}, logger); err != nil {
		t.Fatal(err)
	}
	value, err := readSecretFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("web secret length = %d, %v", len(decoded), err)
	}
	if err := run([]string{"web-keygen", "--out", path}, logger); err == nil {
		t.Fatal("web-keygen overwrote an existing secret")
	}
}

func TestPublicURLAndRoleValidation(t *testing.T) {
	for _, value := range []string{"https://tracker.example", "https://tracker.example/"} {
		if err := validatePublicURL(value, false); err != nil {
			t.Fatalf("valid URL %q: %v", value, err)
		}
	}
	for _, value := range []string{
		"http://tracker.example", "https://user@tracker.example",
		"https://tracker.example?q=1", "https://tracker.example/base",
	} {
		if err := validatePublicURL(value, false); err == nil {
			t.Fatalf("invalid URL accepted: %q", value)
		}
	}
	roles, err := parseRoles("worker,shard_owner,worker")
	if err != nil || len(roles) != 2 || !roles[tracker.RoleWorker] || !roles[tracker.RoleShardOwner] {
		t.Fatalf("roles = %+v, %v", roles, err)
	}
	if _, err := parseRoles("superuser"); err == nil {
		t.Fatal("unknown role accepted")
	}
}

func TestSecretFileRequiresPrivatePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("machine-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := readSecretFile(path)
	if err != nil || value != "machine-token" {
		t.Fatalf("secret = %q, %v", value, err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readSecretFile(path); err == nil {
		t.Fatal("group-readable token accepted")
	}
}

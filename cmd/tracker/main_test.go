package main

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRoleValidation(t *testing.T) {
	roles, err := parseRoles("worker,admin,worker")
	if err != nil || len(roles) != 2 {
		t.Fatalf("roles = %+v, %v", roles, err)
	}
	if _, err := parseRoles("shard_owner"); err == nil {
		t.Fatal("legacy shard role accepted")
	}
}

func TestWebKeygenCreatesPrivateSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "web.secret")
	if err := runWebKeygen([]string{"--out", path}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("secret mode = %o", info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(decoded) != 32 {
		t.Fatalf("secret length = %d, %v", len(decoded), err)
	}
	if err := runWebKeygen([]string{"--out", path}); err == nil {
		t.Fatal("web-keygen overwrote an existing secret")
	}
}

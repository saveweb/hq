package agentidentity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateLoadAndKindIsolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard", "identity.json")
	created, err := Create(path, "shard", 1_780_000_000)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path, "shard")
	if err != nil {
		t.Fatal(err)
	}
	if loaded != created {
		t.Fatalf("loaded = %+v, created = %+v", loaded, created)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "local_admin_token") {
		t.Fatal("stable identity file contains a runtime local admin token")
	}
	if _, err := Load(path, "worker"); err == nil {
		t.Fatal("shard identity accepted as worker identity")
	}
	if _, err := Create(path, "shard", 1_780_000_001); err == nil {
		t.Fatal("identity creation overwrote an existing file")
	}
}

func TestVersionOneIdentityStillLoadsWithoutReusingItsAdminToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.json")
	legacy := `{"version":1,"agent_id":"sh_legacy","kind":"shard","local_admin_token":"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ","created_at":1780000000}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	identity, err := Load(path, "shard")
	if err != nil || identity.AgentID != "sh_legacy" {
		t.Fatalf("legacy identity = %+v, %v", identity, err)
	}
}

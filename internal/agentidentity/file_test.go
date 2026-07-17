package agentidentity

import (
	"path/filepath"
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
	if loaded != created || len(loaded.LocalAdminToken) < 43 {
		t.Fatalf("loaded = %+v, created = %+v", loaded, created)
	}
	if _, err := Load(path, "worker"); err == nil {
		t.Fatal("shard identity accepted as worker identity")
	}
	if _, err := Create(path, "shard", 1_780_000_001); err == nil {
		t.Fatal("identity creation overwrote an existing file")
	}
}

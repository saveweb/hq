package signingkey

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys", "tracker.json")
	created, err := Create(path, "key-1", 1_780_000_000)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.KeyID != created.KeyID || loaded.CreatedAt != created.CreatedAt ||
		!loaded.PrivateKey.Equal(created.PrivateKey) {
		t.Fatalf("loaded key does not match created key")
	}
	if _, err := Create(path, "key-2", 1_780_000_001); err == nil {
		t.Fatal("Create overwrote an existing key")
	}
}

func TestLoadRejectsLoosePermissionsAndUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tracker.json")
	if _, err := Create(path, "key-1", 1_780_000_000); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load accepted a group-readable key")
	}
}

package tlsidentity

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateLoadAndPin(t *testing.T) {
	directory := t.TempDir()
	keyPath := filepath.Join(directory, "private", "shard.key")
	certificatePath := filepath.Join(directory, "shard.crt")
	pin, err := Create(keyPath, certificatePath, "shard.example", time.Now(), DefaultValidity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tls.LoadX509KeyPair(certificatePath, keyPath); err != nil {
		t.Fatal(err)
	}
	loadedPin, err := PinFromCertificateFile(certificatePath)
	if err != nil || loadedPin != pin || len(pin) != 43 {
		t.Fatalf("pin = %q, loaded = %q, err = %v", pin, loadedPin, err)
	}
	info, err := os.Stat(keyPath)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("key permissions = %v, %v", info.Mode().Perm(), err)
	}
	if _, err := Create(keyPath, certificatePath+".new", "shard.example", time.Now(), DefaultValidity); err == nil {
		t.Fatal("Create overwrote an existing key")
	}
}

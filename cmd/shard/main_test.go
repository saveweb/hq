package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestServeConfigAndEndpointAreExplicit(t *testing.T) {
	config, err := parseServeConfig([]string{
		"--tracker-url", "https://tracker.test", "--machine-token-file", "token",
		"--identity-file", "identity", "--endpoint", "https://shard.test/edge",
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.trackerIssuer != config.trackerURL || config.endpointVersion != 1 {
		t.Fatalf("config = %+v", config)
	}
	if _, err := validateEndpoint("http://shard.test", false); err == nil {
		t.Fatal("HTTP endpoint accepted without opt-in")
	}
	if _, err := validateEndpoint("http://shard.test", true); err != nil {
		t.Fatal(err)
	}
	if _, err := validateEndpoint("https://shard.test?q=1", false); err == nil {
		t.Fatal("endpoint query accepted")
	}
	endpoint, _ := validateEndpoint("https://shard.test", false)
	if err := validateTLSMode(config, endpoint); err == nil {
		t.Fatal("HTTPS endpoint accepted without direct TLS or proxy declaration")
	}
	config.tlsTerminatedByProxy = true
	if err := validateTLSMode(config, endpoint); err != nil {
		t.Fatal(err)
	}
}

func TestMachineTokenFileRequiresPrivatePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("machine-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := readSecretFile(path)
	if err != nil || value != "machine-token" {
		t.Fatalf("token = %q, %v", value, err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readSecretFile(path); err == nil {
		t.Fatal("group-readable token accepted")
	}
}

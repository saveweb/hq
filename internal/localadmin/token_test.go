package localadmin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeTokenRotatesAndExplicitTokenStaysInMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime", "admin.token")
	first, err := ResolveToken("", path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ResolveToken("", path)
	if err != nil {
		t.Fatal(err)
	}
	if first.Token == second.Token || len(second.Token) < 43 || second.FilePath != path {
		t.Fatalf("tokens did not rotate: first=%d second=%d", len(first.Token), len(second.Token))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("token permissions = %o", info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil || strings.TrimSpace(string(raw)) != second.Token {
		t.Fatalf("stored token mismatch: %v", err)
	}
	explicit := strings.Repeat("x", 32)
	fromEnv, err := ResolveToken(explicit, filepath.Join(t.TempDir(), "unused"))
	if err != nil || !fromEnv.FromEnv || fromEnv.Token != explicit || fromEnv.FilePath != "" {
		t.Fatalf("explicit result = %+v, %v", fromEnv, err)
	}
	if _, err := ResolveToken("short", "unused"); err == nil {
		t.Fatal("short explicit token accepted")
	}
}

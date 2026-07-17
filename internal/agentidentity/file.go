// Package agentidentity persists stable daemon identity and its local-only
// administration token.
package agentidentity

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const fileVersion = 2

type Identity struct {
	AgentID   string
	Kind      string
	CreatedAt int64
}

type document struct {
	Version         int    `json:"version"`
	AgentID         string `json:"agent_id"`
	Kind            string `json:"kind"`
	LocalAdminToken string `json:"local_admin_token,omitempty"`
	CreatedAt       int64  `json:"created_at"`
}

func Create(path, kind string, createdAt int64) (Identity, error) {
	if path == "" || (kind != "shard" && kind != "worker") || createdAt < 1 {
		return Identity{}, fmt.Errorf("agent identity: invalid creation parameters")
	}
	agentRandom, err := random(18)
	if err != nil {
		return Identity{}, err
	}
	prefix := "sh_"
	if kind == "worker" {
		prefix = "wk_"
	}
	value := Identity{
		AgentID: prefix + agentRandom, Kind: kind, CreatedAt: createdAt,
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return Identity{}, fmt.Errorf("agent identity: create directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return Identity{}, fmt.Errorf("agent identity: create file: %w", err)
	}
	remove := true
	defer func() {
		_ = file.Close()
		if remove {
			_ = os.Remove(path)
		}
	}()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(document{
		Version: fileVersion, AgentID: value.AgentID, Kind: kind, CreatedAt: createdAt,
	}); err != nil {
		return Identity{}, fmt.Errorf("agent identity: write: %w", err)
	}
	if err := file.Sync(); err != nil {
		return Identity{}, fmt.Errorf("agent identity: sync: %w", err)
	}
	if err := file.Close(); err != nil {
		return Identity{}, fmt.Errorf("agent identity: close: %w", err)
	}
	remove = false
	return value, nil
}

func Load(path, expectedKind string) (Identity, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Identity{}, fmt.Errorf("agent identity: stat: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return Identity{}, fmt.Errorf("agent identity: file permissions must not allow group or other access")
	}
	file, err := os.Open(path)
	if err != nil {
		return Identity{}, fmt.Errorf("agent identity: open: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 16<<10))
	decoder.DisallowUnknownFields()
	var value document
	if err := decoder.Decode(&value); err != nil {
		return Identity{}, fmt.Errorf("agent identity: decode: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Identity{}, fmt.Errorf("agent identity: expected one JSON object")
	}
	validVersion := value.Version == fileVersion ||
		(value.Version == 1 && len(value.LocalAdminToken) >= 43)
	if !validVersion || (value.Version == fileVersion && value.LocalAdminToken != "") ||
		value.Kind != expectedKind || value.AgentID == "" || value.CreatedAt < 1 {
		return Identity{}, fmt.Errorf("agent identity: invalid identity document")
	}
	return Identity{
		AgentID: value.AgentID, Kind: value.Kind, CreatedAt: value.CreatedAt,
	}, nil
}

func random(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("agent identity: random: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

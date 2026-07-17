// Package signingkey reads and creates the tracker's Ed25519 signing key file.
package signingkey

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const fileVersion = 1

type document struct {
	Version           int    `json:"version"`
	KeyID             string `json:"key_id"`
	PrivateKeyEd25519 string `json:"private_key_ed25519"`
	CreatedAt         int64  `json:"created_at"`
}

type Key struct {
	KeyID      string
	PrivateKey ed25519.PrivateKey
	CreatedAt  int64
}

func Create(path, keyID string, createdAt int64) (Key, error) {
	if path == "" || keyID == "" || len(keyID) > 128 || createdAt < 1 {
		return Key{}, fmt.Errorf("signing key: invalid creation parameters")
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Key{}, fmt.Errorf("signing key: generate: %w", err)
	}
	document := document{
		Version: fileVersion, KeyID: keyID,
		PrivateKeyEd25519: base64.RawURLEncoding.EncodeToString(privateKey), CreatedAt: createdAt,
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return Key{}, fmt.Errorf("signing key: create directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return Key{}, fmt.Errorf("signing key: create file: %w", err)
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
	if err := encoder.Encode(document); err != nil {
		return Key{}, fmt.Errorf("signing key: write: %w", err)
	}
	if err := file.Sync(); err != nil {
		return Key{}, fmt.Errorf("signing key: sync: %w", err)
	}
	if err := file.Close(); err != nil {
		return Key{}, fmt.Errorf("signing key: close: %w", err)
	}
	remove = false
	return Key{KeyID: keyID, PrivateKey: privateKey, CreatedAt: createdAt}, nil
}

func Load(path string) (Key, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Key{}, fmt.Errorf("signing key: stat: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return Key{}, fmt.Errorf("signing key: file permissions must not allow group or other access")
	}
	file, err := os.Open(path)
	if err != nil {
		return Key{}, fmt.Errorf("signing key: open: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 16<<10))
	decoder.DisallowUnknownFields()
	var value document
	if err := decoder.Decode(&value); err != nil {
		return Key{}, fmt.Errorf("signing key: decode: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Key{}, fmt.Errorf("signing key: expected one JSON object")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value.PrivateKeyEd25519)
	if value.Version != fileVersion || value.KeyID == "" || len(value.KeyID) > 128 || value.CreatedAt < 1 ||
		err != nil || len(decoded) != ed25519.PrivateKeySize {
		return Key{}, fmt.Errorf("signing key: invalid key document")
	}
	return Key{KeyID: value.KeyID, PrivateKey: ed25519.PrivateKey(decoded), CreatedAt: value.CreatedAt}, nil
}

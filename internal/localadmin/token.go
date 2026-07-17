package localadmin

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

const minTokenLength = 32

type TokenResult struct {
	Token    string
	FilePath string
	FromEnv  bool
}

// ResolveToken uses an explicit environment value when present. Otherwise it
// rotates a random runtime token and atomically writes it to a private file.
func ResolveToken(explicit, filePath string) (TokenResult, error) {
	if explicit != "" {
		if err := validateToken(explicit); err != nil {
			return TokenResult{}, err
		}
		return TokenResult{Token: explicit, FromEnv: true}, nil
	}
	if filePath == "" {
		return TokenResult{}, fmt.Errorf("local admin: runtime token file is required")
	}
	token, err := GenerateToken()
	if err != nil {
		return TokenResult{}, err
	}
	if err := writePrivateAtomic(filePath, token+"\n"); err != nil {
		return TokenResult{}, err
	}
	return TokenResult{Token: token, FilePath: filePath}, nil
}

func GenerateToken() (string, error) {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("local admin: random token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(random[:]), nil
}

func validateToken(value string) error {
	if len(value) < minTokenLength || len(value) > 1024 || strings.TrimSpace(value) != value {
		return fmt.Errorf("local admin: explicit token must be 32-1024 characters without surrounding whitespace")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("local admin: explicit token contains control characters")
		}
	}
	return nil
}

func writePrivateAtomic(path, value string) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("local admin: create runtime directory: %w", err)
	}
	file, err := os.CreateTemp(directory, ".local-admin-token-*")
	if err != nil {
		return fmt.Errorf("local admin: create token file: %w", err)
	}
	temporary := file.Name()
	remove := true
	defer func() {
		_ = file.Close()
		if remove {
			_ = os.Remove(temporary)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("local admin: protect token file: %w", err)
	}
	if _, err := file.WriteString(value); err != nil {
		return fmt.Errorf("local admin: write token file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("local admin: sync token file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("local admin: close token file: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("local admin: install token file: %w", err)
	}
	remove = false
	return nil
}

package sqlitequeue

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

func newAttemptID() (string, error) {
	var random [18]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate attempt ID: %w", err)
	}
	return "at_" + base64.RawURLEncoding.EncodeToString(random[:]), nil
}

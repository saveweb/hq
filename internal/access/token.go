// Package access signs and verifies the narrow shard access token used by
// SavewebHQ. It is intentionally not a general-purpose JWT implementation.
package access

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	Audience       = "saveweb-shard"
	MaxTokenBytes  = 4096
	MaxTTLSeconds  = int64(600)
	DefaultSkewSec = int64(30)
)

var (
	ErrMalformed      = errors.New("malformed access token")
	ErrUnknownKey     = errors.New("unknown access token key")
	ErrBadSignature   = errors.New("invalid access token signature")
	ErrExpired        = errors.New("access token expired")
	ErrNotYetValid    = errors.New("access token not yet valid")
	ErrSessionExpired = errors.New("worker session expired")
	ErrScope          = errors.New("access token scope mismatch")
)

type Header struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid"`
}

type Claims struct {
	Issuer           string `json:"iss"`
	Audience         string `json:"aud"`
	Subject          string `json:"sub"`
	TokenID          string `json:"jti"`
	IssuedAt         int64  `json:"iat"`
	NotBefore        int64  `json:"nbf"`
	ExpiresAt        int64  `json:"exp"`
	WorkerAgentID    string `json:"worker_agent_id"`
	SessionID        string `json:"session_id"`
	SessionExpiresAt int64  `json:"session_expires_at"`
	ProjectID        string `json:"project_id"`
	ShardID          string `json:"shard_id"`
	Generation       int64  `json:"generation"`
	OwnerAgentID     string `json:"owner_agent_id"`
}

type Scope struct {
	WorkerAgentID string
	SessionID     string
	ProjectID     string
	ShardID       string
	Generation    int64
	OwnerAgentID  string
}

type Signer struct {
	issuer     string
	keyID      string
	privateKey ed25519.PrivateKey
	now        func() int64
}

func NewSigner(issuer, keyID string, privateKey ed25519.PrivateKey, now func() int64) (*Signer, error) {
	if issuer == "" || keyID == "" || len(privateKey) != ed25519.PrivateKeySize || now == nil {
		return nil, fmt.Errorf("access: invalid signer configuration")
	}
	return &Signer{issuer: issuer, keyID: keyID, privateKey: privateKey, now: now}, nil
}

func GenerateKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

func (s *Signer) KeyID() string { return s.keyID }

func (s *Signer) PublicKey() ed25519.PublicKey {
	publicKey := s.privateKey.Public().(ed25519.PublicKey)
	return append(ed25519.PublicKey(nil), publicKey...)
}

func (s *Signer) Sign(scope Scope, sessionExpiresAt, ttlSeconds int64) (string, Claims, error) {
	if err := validateScope(scope); err != nil {
		return "", Claims{}, err
	}
	if ttlSeconds < 1 || ttlSeconds > MaxTTLSeconds {
		return "", Claims{}, fmt.Errorf("access: TTL must be between 1 and %d seconds", MaxTTLSeconds)
	}
	now := s.now()
	if sessionExpiresAt <= now {
		return "", Claims{}, ErrSessionExpired
	}
	tokenID, err := randomTokenID()
	if err != nil {
		return "", Claims{}, err
	}
	claims := Claims{
		Issuer: s.issuer, Audience: Audience, Subject: scope.WorkerAgentID,
		TokenID: tokenID, IssuedAt: now, NotBefore: now - DefaultSkewSec,
		ExpiresAt: now + ttlSeconds, WorkerAgentID: scope.WorkerAgentID,
		SessionID: scope.SessionID, SessionExpiresAt: sessionExpiresAt,
		ProjectID: scope.ProjectID, ShardID: scope.ShardID,
		Generation: scope.Generation, OwnerAgentID: scope.OwnerAgentID,
	}
	header := Header{Algorithm: "EdDSA", Type: "JWT", KeyID: s.keyID}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", Claims{}, fmt.Errorf("access: encode header: %w", err)
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", Claims{}, fmt.Errorf("access: encode claims: %w", err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(headerJSON)
	encodedClaims := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signed := encodedHeader + "." + encodedClaims
	signature := ed25519.Sign(s.privateKey, []byte(signed))
	token := signed + "." + base64.RawURLEncoding.EncodeToString(signature)
	if len(token) > MaxTokenBytes {
		return "", Claims{}, ErrMalformed
	}
	return token, claims, nil
}

type Verifier struct {
	issuer string
	keys   map[string]ed25519.PublicKey
	now    func() int64
	skew   int64
}

func NewVerifier(issuer string, keys map[string]ed25519.PublicKey, now func() int64, skewSeconds int64) (*Verifier, error) {
	if issuer == "" || now == nil || skewSeconds < 0 || skewSeconds > 300 {
		return nil, fmt.Errorf("access: invalid verifier configuration")
	}
	copyKeys := make(map[string]ed25519.PublicKey, len(keys))
	for keyID, key := range keys {
		if keyID == "" || len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("access: invalid public key")
		}
		copyKeys[keyID] = append(ed25519.PublicKey(nil), key...)
	}
	return &Verifier{issuer: issuer, keys: copyKeys, now: now, skew: skewSeconds}, nil
}

func (v *Verifier) Verify(token string, scope Scope) (Claims, error) {
	if len(token) == 0 || len(token) > MaxTokenBytes || strings.Count(token, ".") != 2 {
		return Claims{}, ErrMalformed
	}
	parts := strings.Split(token, ".")
	headerJSON, err := decodeSegment(parts[0], 1024)
	if err != nil {
		return Claims{}, ErrMalformed
	}
	var header Header
	if err := decodeStrict(headerJSON, &header); err != nil || header.Algorithm != "EdDSA" || header.Type != "JWT" || header.KeyID == "" {
		return Claims{}, ErrMalformed
	}
	publicKey, ok := v.keys[header.KeyID]
	if !ok {
		return Claims{}, ErrUnknownKey
	}
	signature, err := decodeSegment(parts[2], ed25519.SignatureSize)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return Claims{}, ErrMalformed
	}
	if !ed25519.Verify(publicKey, []byte(parts[0]+"."+parts[1]), signature) {
		return Claims{}, ErrBadSignature
	}
	claimsJSON, err := decodeSegment(parts[1], 3072)
	if err != nil {
		return Claims{}, ErrMalformed
	}
	var claims Claims
	if err := decodeStrict(claimsJSON, &claims); err != nil {
		return Claims{}, ErrMalformed
	}
	if err := validateClaims(claims, v.issuer); err != nil {
		return Claims{}, err
	}
	now := v.now()
	if now+v.skew < claims.NotBefore {
		return Claims{}, ErrNotYetValid
	}
	if now-v.skew >= claims.ExpiresAt {
		return Claims{}, ErrExpired
	}
	if now >= claims.SessionExpiresAt {
		return Claims{}, ErrSessionExpired
	}
	if err := validateScope(scope); err != nil || !matchesScope(claims, scope) {
		return Claims{}, ErrScope
	}
	return claims, nil
}

func validateClaims(claims Claims, issuer string) error {
	if claims.Issuer != issuer || claims.Audience != Audience || claims.Subject == "" ||
		claims.Subject != claims.WorkerAgentID || claims.TokenID == "" ||
		claims.IssuedAt < 0 || claims.NotBefore < 0 || claims.NotBefore > claims.IssuedAt ||
		claims.ExpiresAt <= claims.IssuedAt ||
		claims.ExpiresAt-claims.IssuedAt > MaxTTLSeconds || claims.SessionExpiresAt <= 0 {
		return ErrMalformed
	}
	return validateScope(Scope{
		WorkerAgentID: claims.WorkerAgentID, SessionID: claims.SessionID,
		ProjectID: claims.ProjectID, ShardID: claims.ShardID,
		Generation: claims.Generation, OwnerAgentID: claims.OwnerAgentID,
	})
}

func validateScope(scope Scope) error {
	if scope.WorkerAgentID == "" || scope.SessionID == "" || scope.ProjectID == "" ||
		scope.ShardID == "" || scope.Generation < 1 || scope.OwnerAgentID == "" {
		return ErrScope
	}
	return nil
}

func matchesScope(claims Claims, scope Scope) bool {
	return claims.WorkerAgentID == scope.WorkerAgentID && claims.SessionID == scope.SessionID &&
		claims.ProjectID == scope.ProjectID && claims.ShardID == scope.ShardID &&
		claims.Generation == scope.Generation && claims.OwnerAgentID == scope.OwnerAgentID
}

func decodeSegment(value string, maxDecoded int) ([]byte, error) {
	if value == "" || base64.RawURLEncoding.DecodedLen(len(value)) > maxDecoded {
		return nil, ErrMalformed
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) > maxDecoded {
		return nil, ErrMalformed
	}
	return decoded, nil
}

func decodeStrict(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ErrMalformed
	}
	return nil
}

func randomTokenID() (string, error) {
	var value [18]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("access: generate token ID: %w", err)
	}
	return "jt_" + base64.RawURLEncoding.EncodeToString(value[:]), nil
}

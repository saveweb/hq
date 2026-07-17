package access

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

const tokenNow = int64(1_780_000_000)

func testScope() Scope {
	return Scope{
		WorkerAgentID: "agent-worker-1", SessionID: "session-1",
		ProjectID: "project-1", ShardID: "shard-1", Generation: 7,
		OwnerAgentID: "agent-shard-1",
	}
}

func testSigner(t *testing.T) (*Signer, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewSigner("https://tracker.example", "key-1", privateKey, func() int64 { return tokenNow })
	if err != nil {
		t.Fatal(err)
	}
	return signer, publicKey
}

func TestSignAndVerifyScopedToken(t *testing.T) {
	signer, publicKey := testSigner(t)
	token, signedClaims, err := signer.Sign(testScope(), tokenNow+120, 60)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier("https://tracker.example", map[string]ed25519.PublicKey{
		"key-1": publicKey,
	}, func() int64 { return tokenNow + 1 }, DefaultSkewSec)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(token, testScope())
	if err != nil {
		t.Fatal(err)
	}
	if claims != signedClaims || claims.Subject != testScope().WorkerAgentID {
		t.Fatalf("verified claims = %+v, signed = %+v", claims, signedClaims)
	}

	wrongScope := testScope()
	wrongScope.Generation++
	if _, err := verifier.Verify(token, wrongScope); !errors.Is(err, ErrScope) {
		t.Fatalf("wrong scope error = %v", err)
	}
}

func TestVerifierRejectsTamperingUnknownFieldsAndAlgorithms(t *testing.T) {
	signer, publicKey := testSigner(t)
	token, _, err := signer.Sign(testScope(), tokenNow+120, 60)
	if err != nil {
		t.Fatal(err)
	}
	verifier, _ := NewVerifier("https://tracker.example", map[string]ed25519.PublicKey{"key-1": publicKey},
		func() int64 { return tokenNow }, DefaultSkewSec)

	parts := strings.Split(token, ".")
	parts[1] = parts[1][:len(parts[1])-1] + "A"
	if _, err := verifier.Verify(strings.Join(parts, "."), testScope()); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("tampered token error = %v", err)
	}

	headerJSON, _ := json.Marshal(map[string]any{"alg": "none", "typ": "JWT", "kid": "key-1"})
	parts = strings.Split(token, ".")
	parts[0] = base64.RawURLEncoding.EncodeToString(headerJSON)
	if _, err := verifier.Verify(strings.Join(parts, "."), testScope()); !errors.Is(err, ErrMalformed) {
		t.Fatalf("algorithm confusion error = %v", err)
	}

	headerJSON, _ = json.Marshal(map[string]any{"alg": "EdDSA", "typ": "JWT", "kid": "key-1", "extra": true})
	parts[0] = base64.RawURLEncoding.EncodeToString(headerJSON)
	if _, err := verifier.Verify(strings.Join(parts, "."), testScope()); !errors.Is(err, ErrMalformed) {
		t.Fatalf("unknown header error = %v", err)
	}
}

func TestVerifierDistinguishesTokenAndSessionExpiry(t *testing.T) {
	signer, publicKey := testSigner(t)
	token, _, err := signer.Sign(testScope(), tokenNow+20, 10)
	if err != nil {
		t.Fatal(err)
	}
	verifier, _ := NewVerifier("https://tracker.example", map[string]ed25519.PublicKey{"key-1": publicKey},
		func() int64 { return tokenNow + 41 }, DefaultSkewSec)
	if _, err := verifier.Verify(token, testScope()); !errors.Is(err, ErrExpired) {
		t.Fatalf("token expiry error = %v", err)
	}

	token, _, err = signer.Sign(testScope(), tokenNow+5, 60)
	if err != nil {
		t.Fatal(err)
	}
	verifier, _ = NewVerifier("https://tracker.example", map[string]ed25519.PublicKey{"key-1": publicKey},
		func() int64 { return tokenNow + 5 }, DefaultSkewSec)
	if _, err := verifier.Verify(token, testScope()); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("session expiry error = %v", err)
	}
}

func TestSignerRejectsInvalidTTLAndExpiredSession(t *testing.T) {
	signer, _ := testSigner(t)
	if _, _, err := signer.Sign(testScope(), tokenNow+10, MaxTTLSeconds+1); err == nil {
		t.Fatal("signer accepted excessive TTL")
	}
	if _, _, err := signer.Sign(testScope(), tokenNow, 10); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("expired session error = %v", err)
	}
}

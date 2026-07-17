// Package tlsidentity creates the stable P-256 key and renewable self-signed
// certificate used by a directly exposed shard endpoint.
package tlsidentity

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const DefaultValidity = 365 * 24 * time.Hour

func Create(keyPath, certificatePath, serverName string, now time.Time, validity time.Duration) (string, error) {
	if keyPath == "" || certificatePath == "" || keyPath == certificatePath || serverName == "" ||
		validity < 24*time.Hour || validity > 397*24*time.Hour {
		return "", fmt.Errorf("TLS identity: invalid creation parameters")
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", fmt.Errorf("TLS identity: generate key: %w", err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return "", fmt.Errorf("TLS identity: generate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: serverName},
		NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(validity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	if address := net.ParseIP(serverName); address != nil {
		template.IPAddresses = []net.IP{address}
	} else {
		template.DNSNames = []string{serverName}
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return "", fmt.Errorf("TLS identity: create certificate: %w", err)
	}
	createdCertificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		return "", fmt.Errorf("TLS identity: parse created certificate: %w", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return "", fmt.Errorf("TLS identity: marshal key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return "", fmt.Errorf("TLS identity: create key directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(certificatePath), 0o755); err != nil {
		return "", fmt.Errorf("TLS identity: create certificate directory: %w", err)
	}
	keyFile, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("TLS identity: create key file: %w", err)
	}
	removeKey := true
	defer func() {
		_ = keyFile.Close()
		if removeKey {
			_ = os.Remove(keyPath)
		}
	}()
	if err := pem.Encode(keyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}); err != nil {
		return "", fmt.Errorf("TLS identity: write key: %w", err)
	}
	if err := keyFile.Sync(); err != nil {
		return "", fmt.Errorf("TLS identity: sync key: %w", err)
	}
	if err := keyFile.Close(); err != nil {
		return "", fmt.Errorf("TLS identity: close key: %w", err)
	}
	certificateFile, err := os.OpenFile(certificatePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", fmt.Errorf("TLS identity: create certificate file: %w", err)
	}
	removeCertificate := true
	defer func() {
		_ = certificateFile.Close()
		if removeCertificate {
			_ = os.Remove(certificatePath)
		}
	}()
	if err := pem.Encode(certificateFile, &pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}); err != nil {
		return "", fmt.Errorf("TLS identity: write certificate: %w", err)
	}
	if err := certificateFile.Sync(); err != nil {
		return "", fmt.Errorf("TLS identity: sync certificate: %w", err)
	}
	if err := certificateFile.Close(); err != nil {
		return "", fmt.Errorf("TLS identity: close certificate: %w", err)
	}
	removeKey = false
	removeCertificate = false
	return pinForCertificate(createdCertificate), nil
}

func PinFromCertificateFile(path string) (string, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("TLS identity: read certificate: %w", err)
	}
	block, _ := pem.Decode(encoded)
	if block == nil || block.Type != "CERTIFICATE" {
		return "", fmt.Errorf("TLS identity: invalid certificate PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("TLS identity: parse certificate: %w", err)
	}
	return pinForCertificate(certificate), nil
}

func pinForCertificate(certificate *x509.Certificate) string {
	digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

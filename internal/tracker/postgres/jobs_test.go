package postgres

import (
	"testing"

	"github.com/saveweb/hq/pkg/protocol"
)

func TestValidateArtifactReceiptChecksum(t *testing.T) {
	base := protocol.ArtifactReceipt{
		ID: "receipt-1", Issuer: "https://canner.example", ObjectID: "object-1",
		SizeBytes: 1, AcceptedAt: 1,
	}
	for _, checksum := range []string{
		"blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"sha1:0123456789abcdef0123456789abcdef01234567",
		"md5:0123456789abcdef0123456789abcdef",
		"xx128hash:0123456789abcdef0123456789abcdef",
	} {
		receipt := base
		receipt.Checksum = checksum
		if err := validateArtifactReceipts([]protocol.ArtifactReceipt{receipt}); err != nil {
			t.Errorf("checksum %q rejected: %v", checksum, err)
		}
	}
	for _, checksum := range []string{
		"", "blake3", "BLAKE3:0123", "blake3:ABCDEF", "blake3:0", "blake3:xyz0",
	} {
		receipt := base
		receipt.Checksum = checksum
		if err := validateArtifactReceipts([]protocol.ArtifactReceipt{receipt}); err == nil {
			t.Errorf("invalid checksum %q accepted", checksum)
		}
	}
}

package protocol_test

import (
	"encoding/json"
	"os"
	"testing"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestDefaultJobIDConformance(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("../../api/testdata/job-id-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Type  string `json:"type"`
		Value string `json:"value"`
		ID    string `json:"id"`
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, vector := range vectors {
		vector := vector
		t.Run(vector.Type+"/"+vector.Value, func(t *testing.T) {
			t.Parallel()
			if got := protocol.DefaultJobID(vector.Type, vector.Value); got != vector.ID {
				t.Fatalf("DefaultJobID(%q, %q) = %q, want %q", vector.Type, vector.Value, got, vector.ID)
			}
		})
	}
}

func TestDefaultJobIDUsesSeedForEmptyType(t *testing.T) {
	t.Parallel()
	want := protocol.DefaultJobID(protocol.JobTypeSeed, "https://example.org/")
	if got := protocol.DefaultJobID("", "https://example.org/"); got != want {
		t.Fatalf("empty type ID = %q, want %q", got, want)
	}
}

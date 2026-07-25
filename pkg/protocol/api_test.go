package protocol

import (
	"encoding/json"
	"testing"
)

func TestOutcomeKindValid(t *testing.T) {
	for _, kind := range []OutcomeKind{OutcomeSuccess, OutcomeHTTPError, OutcomeSkipped} {
		if !kind.Valid() {
			t.Fatalf("expected %q to be valid", kind)
		}
	}
	if OutcomeKind("archived").Valid() {
		t.Fatal("expected unknown outcome kind to be invalid")
	}
}

func TestOutcomeKindJSON(t *testing.T) {
	encoded, err := json.Marshal(Outcome{Kind: OutcomeSuccess, Meta: Attrs{}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"kind":"success","code":null,"uri":null,"meta":{}}`; got != want {
		t.Fatalf("encoded outcome = %s, want %s", got, want)
	}
}

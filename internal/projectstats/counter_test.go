package projectstats

import "testing"

func TestCounterWindowsAndRotation(t *testing.T) {
	c := New()
	if got := c.Snapshot("missing", 100); got != (Snapshot{}) {
		t.Fatalf("empty snapshot = %+v", got)
	}
	c.AddClaimed("demo", 100, 5)
	c.AddCompleted("demo", 100, 3)
	c.AddClaimed("demo", 91, 7)
	c.AddCompleted("other", 100, 99)
	got := c.Snapshot("demo", 100)
	if got.Claimed10 != 12 || got.Claimed60 != 12 || got.Completed10 != 3 || got.Completed60 != 3 {
		t.Fatalf("snapshot = %+v", got)
	}
	c.AddCompleted("demo", 160, 11)
	got = c.Snapshot("demo", 160)
	if got.Claimed60 != 0 || got.Completed60 != 11 {
		t.Fatalf("rotated snapshot = %+v", got)
	}
}

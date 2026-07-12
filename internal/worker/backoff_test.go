package worker

import (
	"testing"
	"time"
)

// TestBackoffGrowsExponentially checks that, ignoring jitter, each attempt's
// wait is roughly double the previous one until it hits the cap.
func TestBackoffGrowsExponentially(t *testing.T) {
	base := 100 * time.Millisecond
	max := 10 * time.Second

	// Expected center values (before jitter): base * 2^(attempt-1).
	// attempt 1 -> 100ms, 2 -> 200ms, 3 -> 400ms, 4 -> 800ms.
	centers := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
	}

	for i, center := range centers {
		attempt := i + 1
		got := backoff(attempt, base, max)

		// Jitter is +/-20%, so the result must land within that band.
		low := time.Duration(float64(center) * 0.8)
		high := time.Duration(float64(center) * 1.2)
		if got < low || got > high {
			t.Errorf("backoff(attempt=%d) = %s, want within [%s, %s]", attempt, got, low, high)
		}
	}
}

// TestBackoffRespectsMax checks that the wait never blows past the cap
// (allowing for the +20% jitter on top of the capped value).
func TestBackoffRespectsMax(t *testing.T) {
	base := 1 * time.Second
	max := 5 * time.Second

	// A large attempt would be base*2^19 without the cap — must be clamped.
	got := backoff(20, base, max)

	high := time.Duration(float64(max) * 1.2)
	if got > high {
		t.Errorf("backoff(attempt=20) = %s, exceeds capped max+jitter %s", got, high)
	}
}

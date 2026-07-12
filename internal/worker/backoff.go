package worker

import (
	"math"
	"math/rand"
	"time"
)

// backoff computes how long to wait before a given retry attempt.
//
// This is "exponential backoff with jitter", the standard pattern for retrying
// failed work politely:
//
//   - Exponential: each attempt waits roughly twice as long as the last
//     (base, 2*base, 4*base, ...). This gives a struggling downstream service
//     progressively more room to recover instead of hammering it.
//   - Capped at max: we don't want to wait, say, an hour between retries.
//   - Jitter: a small random wobble (+/- 20%) so that if many jobs fail at the
//     same instant, they don't all retry at the exact same moment and stampede
//     the service again (the "thundering herd" problem).
//
// attempt is 1-based: attempt 1 is the first retry.
func backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	// base * 2^(attempt-1), done in float64 to avoid integer overflow on big attempts.
	d := float64(base) * math.Pow(2, float64(attempt-1))
	if d > float64(max) {
		d = float64(max)
	}

	// Add jitter in the range [-20%, +20%] of d.
	jitter := (rand.Float64()*0.4 - 0.2) * d

	return time.Duration(d + jitter)
}

package loki

import "sort"

// ensureMonotonic sorts entries ascending by timestamp and nudges any colliding
// timestamps forward by one nanosecond so every entry in a stream has a strictly
// increasing, unique time.
//
// Why this matters: AlertManager often fires several alerts in one group with an
// identical StartsAt, which would otherwise produce multiple Loki entries at the
// same nanosecond. Loki silently DROPS a second entry that has the same
// (timestamp, line) within a stream — so without this, alert history would be
// quietly incomplete. Pushing colliding entries one nanosecond apart preserves
// every alert while keeping the stream ordered (Loki requires ascending time).
// The drift is at most one nanosecond per colliding entry: negligible for alert
// history, and it never reorders genuinely distinct timestamps.
//
// It mutates and returns the same slice for convenience.
func ensureMonotonic(values []row) []row {
	if len(values) < 2 {
		return values
	}

	sort.SliceStable(values, func(i, j int) bool {
		return values[i].At.Before(values[j].At)
	})

	for i := 1; i < len(values); i++ {
		if !values[i].At.After(values[i-1].At) {
			values[i].At = values[i-1].At.Add(1)
		}
	}
	return values
}

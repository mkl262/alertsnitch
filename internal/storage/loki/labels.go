package loki

import "regexp"

// defaultAllowedLabels is the built-in set of alert labels promoted to Loki
// stream labels when the operator does not configure an explicit allow-list.
//
// These are deliberately LOW-cardinality dimensions: each one has a small,
// bounded set of distinct values, so the number of resulting Loki streams (the
// Cartesian product of all label values) stays manageable. High-cardinality
// labels such as instance, pod, node and container are intentionally NOT here —
// promoting them would explode the active-stream count and degrade Loki. They
// remain available in the JSON log line, and can additionally be surfaced as
// structured metadata (see metadata.go) for fast filtering without the stream
// cost. Operators who run a small deployment and want them as stream labels can
// still opt in via ALERTSNITCH_LOKI_ALLOWED_LABELS.
var defaultAllowedLabels = []string{
	"severity", "priority", "level", "env", "team",
	"cluster", "namespace", "service", "job",
}

// labelNameRe matches the Prometheus/Loki label-name grammar. A label whose
// name does not match cannot be a valid stream label; if one slips through
// (e.g. from a webhook query parameter like ?source-app=x), Loki rejects the
// ENTIRE push, silently losing every alert in the batch. We validate at the
// single chokepoint where labels are assembled and drop the offenders instead.
var labelNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// isValidLabelName reports whether name is a syntactically valid Loki label
// name. Empty names and names with characters outside [a-zA-Z0-9_] (or a
// leading digit) are rejected.
func isValidLabelName(name string) bool {
	return labelNameRe.MatchString(name)
}

// allowedLabelSet builds a lookup set from the configured labels, falling back
// to defaultAllowedLabels when none are configured.
func allowedLabelSet(configured []string) map[string]bool {
	labels := configured
	if len(labels) == 0 {
		labels = defaultAllowedLabels
	}
	set := make(map[string]bool, len(labels))
	for _, l := range labels {
		set[l] = true
	}
	return set
}

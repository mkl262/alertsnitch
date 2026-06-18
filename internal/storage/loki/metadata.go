package loki

import "github.com/mikehsu0618/alertsnitch/internal"

// metadataLabelKeys are the alert-label keys promoted to Loki structured
// metadata when structured metadata is enabled. They are deliberately the
// high-value *filtering* dimensions that are too high-cardinality to be stream
// labels (see labels.go): attaching them as structured metadata keeps queries
// like `... | pod="x"` fast without parsing the JSON line and without inflating
// the active-stream count. All keys here are valid Loki label names.
var metadataLabelKeys = []string{
	"instance", "pod", "node", "container",
	"namespace", "job", "service", "alertname", "severity",
}

// buildAlertMetadata extracts the structured-metadata map for a single alert.
// It returns nil when there is nothing to attach, so callers can leave the wire
// form as the 2-element tuple. The alert fingerprint is included because it is
// the natural key for exact-alert lookups and de-duplication.
func buildAlertMetadata(alert internal.Alert) map[string]string {
	meta := make(map[string]string, len(metadataLabelKeys)+1)
	for _, key := range metadataLabelKeys {
		if v := alert.Labels[key]; v != "" {
			meta[key] = v
		}
	}
	if alert.Fingerprint != "" {
		meta["fingerprint"] = alert.Fingerprint
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

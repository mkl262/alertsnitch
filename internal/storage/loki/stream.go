package loki

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/mikehsu0618/alertsnitch/internal"
)

func cloneLabels(labels map[string]string) map[string]string {
	clone := make(map[string]string, len(labels))
	for k, v := range labels {
		clone[k] = v
	}
	return clone
}

func groupAlertsByStatus(alerts []internal.Alert) map[string][]internal.Alert {
	byStatus := make(map[string][]internal.Alert)
	for _, alert := range alerts {
		byStatus[alert.Status] = append(byStatus[alert.Status], alert)
	}
	return byStatus
}

// dataToStream converts an alert group into one Loki stream per alert status.
func (c *Client) dataToStream(data *internal.AlertGroup, extraLabels map[string]string) ([]stream, error) {
	if len(data.Alerts) == 0 {
		return nil, fmt.Errorf("no alerts to process")
	}

	byStatus := groupAlertsByStatus(data.Alerts)
	baseLabels := c.buildStreamLabels(data, extraLabels)

	streams := make([]stream, 0, len(byStatus))
	for status, alerts := range byStatus {
		s, err := c.createStreamForStatus(status, alerts, data, baseLabels)
		if err != nil {
			return nil, err
		}
		streams = append(streams, s)
	}
	return streams, nil
}

func (c *Client) createStreamForStatus(status string, alerts []internal.Alert, data *internal.AlertGroup, baseLabels map[string]string) (stream, error) {
	streamLabels := cloneLabels(baseLabels)
	streamLabels["alert_status"] = status

	s := stream{
		Stream: streamLabels,
		Values: make([]row, 0, len(alerts)),
	}

	for _, alert := range alerts {
		// Use the alert's real timestamp rather than time.Now() so history is
		// accurate: StartsAt for firing, EndsAt for resolved when valid.
		timestamp := alert.StartsAt
		if status == "resolved" && !alert.EndsAt.IsZero() && alert.EndsAt.After(alert.StartsAt) {
			timestamp = alert.EndsAt
		}
		if timestamp.IsZero() {
			timestamp = time.Now()
		}

		flattened := FlattenAlertGroup{
			Version:           data.Version,
			GroupKey:          data.GroupKey,
			Receiver:          data.Receiver,
			Status:            data.Status,
			Alert:             alert,
			GroupLabels:       data.GroupLabels,
			CommonLabels:      data.CommonLabels,
			CommonAnnotations: data.CommonAnnotations,
			ExternalURL:       data.ExternalURL,
		}

		jsonData, err := json.Marshal(flattened)
		if err != nil {
			return stream{}, fmt.Errorf("error marshaling FlattenAlertGroup: %w", err)
		}

		var meta map[string]string
		if c.cfg.StructuredMetadata {
			meta = buildAlertMetadata(alert)
		}
		s.Values = append(s.Values, row{At: timestamp, Val: string(jsonData), Meta: meta})
	}

	// Colliding StartsAt values within a group are common; keep every entry by
	// giving each a strictly increasing, unique nanosecond timestamp.
	s.Values = ensureMonotonic(s.Values)
	return s, nil
}

func (c *Client) buildStreamLabels(data *internal.AlertGroup, extraLabels map[string]string) map[string]string {
	streamLabels := make(map[string]string, len(extraLabels)+len(data.CommonLabels)+len(data.GroupLabels)+4)

	// extraLabels are operator-chosen (webhook query parameters) and bypass the
	// allow-list by design, but their NAMES still flow from outside the process,
	// so they are validated below to keep an invalid one (e.g. ?app-id=x) from
	// making Loki reject the whole push.
	for key, value := range extraLabels {
		putStreamLabel(streamLabels, key, value)
	}
	for label, value := range data.CommonLabels {
		if c.allowedLabels[label] {
			putStreamLabel(streamLabels, label, value)
		}
	}
	for label, value := range data.GroupLabels {
		if c.allowedLabels[label] {
			putStreamLabel(streamLabels, label, value)
		}
	}

	streamLabels["service_name"] = "alertsnitch"
	streamLabels["receiver"] = data.Receiver
	streamLabels["status"] = data.Status
	streamLabels["alert_name"] = data.CommonLabels["alertname"]

	return streamLabels
}

// putStreamLabel adds a label only if its name is a valid Loki label name,
// dropping (with a debug log) any that would otherwise cause Loki to reject the
// entire push. Empty values are skipped — an empty stream label is noise.
func putStreamLabel(dst map[string]string, name, value string) {
	if value == "" {
		return
	}
	if !isValidLabelName(name) {
		logrus.Debugf("dropping invalid Loki stream label name %q", name)
		return
	}
	dst[name] = value
}

// streamKey is a deterministic string identity for a label set, used to merge
// entries belonging to the same stream during batching.
func streamKey(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}

	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf strings.Builder
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(k)
		buf.WriteByte(':')
		// Quote the value so a value containing ':' or ',' cannot alias a
		// different label set (label names are already restricted to a safe
		// grammar, but values are free-form).
		buf.WriteString(strconv.Quote(labels[k]))
	}
	buf.WriteByte('}')
	return buf.String()
}

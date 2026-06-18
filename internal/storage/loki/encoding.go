package loki

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/mikehsu0618/alertsnitch/internal"
)

// payload is the body of a Loki push request.
type payload struct {
	Streams []stream `json:"streams"`
}

// stream is a single Loki stream: a set of labels and its log entries.
type stream struct {
	Stream map[string]string `json:"stream"`
	Values []row             `json:"values"`
}

// row is a single Loki log entry. It marshals to Loki's tuple form:
// ["<unix-nanos>", "<line>"] or, when structured metadata is attached,
// ["<unix-nanos>", "<line>", {"<key>": "<value>", ...}] (Loki 3.x).
type row struct {
	At   time.Time
	Val  string
	Meta map[string]string // structured metadata; omitted from the wire when empty
}

func (r row) MarshalJSON() ([]byte, error) {
	ts := strconv.FormatInt(r.At.UnixNano(), 10)
	if len(r.Meta) == 0 {
		return json.Marshal([]string{ts, r.Val})
	}
	// A heterogeneous tuple (two strings then an object) cannot be a typed slice,
	// so it is assembled as []any.
	return json.Marshal([]any{ts, r.Val, r.Meta})
}

func (r *row) UnmarshalJSON(data []byte) error {
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}
	if len(arr) != 2 && len(arr) != 3 {
		return fmt.Errorf("expected array of length 2 or 3, got %d", len(arr))
	}

	var tsStr, val string
	if err := json.Unmarshal(arr[0], &tsStr); err != nil {
		return fmt.Errorf("failed to decode timestamp: %w", err)
	}
	if err := json.Unmarshal(arr[1], &val); err != nil {
		return fmt.Errorf("failed to decode line: %w", err)
	}
	timestamp, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse timestamp: %w", err)
	}

	r.At = time.Unix(0, timestamp)
	r.Val = val
	r.Meta = nil
	if len(arr) == 3 {
		if err := json.Unmarshal(arr[2], &r.Meta); err != nil {
			return fmt.Errorf("failed to decode structured metadata: %w", err)
		}
	}
	return nil
}

// FlattenAlertGroup is one alert denormalized with its group context. It is the
// JSON shape written as a single Loki log line, so each alert is independently
// queryable.
type FlattenAlertGroup struct {
	Version  string `json:"version"`
	GroupKey string `json:"groupKey"`

	Receiver string         `json:"receiver"`
	Status   string         `json:"status"`
	Alert    internal.Alert `json:"alert"`

	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`

	ExternalURL string `json:"externalURL"`
}

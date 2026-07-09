package loki

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mikehsu0618/alertsnitch/internal"
)

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"nil url", Config{}, true},
		{"bad scheme", Config{URL: mustURL(t, "ftp://loki")}, true},
		{"ok http", Config{URL: mustURL(t, "http://loki:3100")}, false},
		{"timeout too short", Config{URL: mustURL(t, "http://loki"), RequestTimeout: time.Millisecond}, true},
		{"timeout too long", Config{URL: mustURL(t, "http://loki"), RequestTimeout: time.Hour}, true},
		{"basic auth missing password", Config{URL: mustURL(t, "http://loki"), Auth: AuthConfig{BasicAuthUser: "u"}}, true},
		{"basic auth missing user", Config{URL: mustURL(t, "http://loki"), Auth: AuthConfig{BasicAuthPassword: "p"}}, true},
		{"client cert without key", Config{URL: mustURL(t, "http://loki"), TLS: TLSConfig{ClientCertPath: "/x"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig_ValidateAppliesDefaultTimeout(t *testing.T) {
	cfg := Config{URL: mustURL(t, "http://loki:3100")}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, defaultTimeout, cfg.RequestTimeout)
}

func TestWALConfig_Validate(t *testing.T) {
	tests := []struct {
		name         string
		wal          WALConfig
		batchEnabled bool
		wantErr      bool
	}{
		{"disabled is always ok", WALConfig{}, false, false},
		{"enabled requires batch", WALConfig{Enabled: true, Dir: "/tmp/x"}, false, true},
		{"enabled requires dir", WALConfig{Enabled: true}, true, true},
		{"enabled with batch and dir ok", WALConfig{Enabled: true, Dir: "/tmp/x"}, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.wal.validate(tt.batchEnabled)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStreamKey_DeterministicAndOrderIndependent(t *testing.T) {
	a := map[string]string{"b": "2", "a": "1", "c": "3"}
	b := map[string]string{"c": "3", "a": "1", "b": "2"}
	assert.Equal(t, streamKey(a), streamKey(b))
	assert.Equal(t, `{a:"1",b:"2",c:"3"}`, streamKey(a))
	assert.Equal(t, "{}", streamKey(nil))
}

// TestStreamKey_ValueEscapingPreventsCollision guards against two distinct label
// sets hashing to the same key when a value contains the ':' or ',' separators.
func TestStreamKey_ValueEscapingPreventsCollision(t *testing.T) {
	one := map[string]string{"a": "x,b:y"}
	two := map[string]string{"a": "x", "b": "y"}
	assert.NotEqual(t, streamKey(one), streamKey(two), "values must not be able to alias a different label set")
}

func TestRowJSON_RoundTrip(t *testing.T) {
	original := row{At: time.Unix(0, 1234567890), Val: "hello"}
	data, err := json.Marshal(original)
	require.NoError(t, err)
	assert.JSONEq(t, `["1234567890","hello"]`, string(data))

	var decoded row
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, original.Val, decoded.Val)
	assert.Equal(t, original.At.UnixNano(), decoded.At.UnixNano())
}

func TestRowUnmarshal_RejectsWrongLength(t *testing.T) {
	var r row
	assert.Error(t, json.Unmarshal([]byte(`["only-one"]`), &r))
	assert.Error(t, json.Unmarshal([]byte(`["a","b","c","d"]`), &r))
}

func TestRowJSON_StructuredMetadataRoundTrip(t *testing.T) {
	original := row{
		At:   time.Unix(0, 1234567890),
		Val:  "hello",
		Meta: map[string]string{"fingerprint": "abc", "pod": "p-1"},
	}
	data, err := json.Marshal(original)
	require.NoError(t, err)
	assert.JSONEq(t, `["1234567890","hello",{"fingerprint":"abc","pod":"p-1"}]`, string(data))

	var decoded row
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, original.Val, decoded.Val)
	assert.Equal(t, original.At.UnixNano(), decoded.At.UnixNano())
	assert.Equal(t, original.Meta, decoded.Meta)
}

func TestRowJSON_EmptyMetadataOmitsThirdElement(t *testing.T) {
	data, err := json.Marshal(row{At: time.Unix(0, 1), Val: "x", Meta: map[string]string{}})
	require.NoError(t, err)
	assert.JSONEq(t, `["1","x"]`, string(data), "empty metadata must not emit a third tuple element")
}

func TestBuildAlertMetadata(t *testing.T) {
	t.Run("promotes high-card labels and fingerprint", func(t *testing.T) {
		meta := buildAlertMetadata(internal.Alert{
			Fingerprint: "fp-1",
			Labels: map[string]string{
				"pod":      "web-abc",
				"instance": "10.0.0.1:9090",
				"severity": "critical",
				"ignored":  "not-promoted",
			},
		})
		assert.Equal(t, "web-abc", meta["pod"])
		assert.Equal(t, "10.0.0.1:9090", meta["instance"])
		assert.Equal(t, "critical", meta["severity"])
		assert.Equal(t, "fp-1", meta["fingerprint"])
		_, ignored := meta["ignored"]
		assert.False(t, ignored, "non-curated label must not be promoted")
	})

	t.Run("nil when nothing to attach", func(t *testing.T) {
		assert.Nil(t, buildAlertMetadata(internal.Alert{Labels: map[string]string{"ignored": "x"}}))
	})
}

func TestDataToStream_StructuredMetadataToggle(t *testing.T) {
	start := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	ag := &internal.AlertGroup{
		Version:      "4",
		Receiver:     "r",
		Status:       "firing",
		CommonLabels: map[string]string{"alertname": "X"},
		Alerts: internal.Alerts{
			{Status: "firing", StartsAt: start, Fingerprint: "fp", Labels: map[string]string{"pod": "p1"}},
		},
	}

	t.Run("disabled by default", func(t *testing.T) {
		streams, err := (&Client{allowedLabels: allowedLabelSet(nil)}).dataToStream(ag, nil, time.Now())
		require.NoError(t, err)
		assert.Nil(t, streams[0].Values[0].Meta, "metadata must be absent when disabled")
	})

	t.Run("enabled attaches metadata", func(t *testing.T) {
		c := &Client{allowedLabels: allowedLabelSet(nil), cfg: Config{StructuredMetadata: true}}
		streams, err := c.dataToStream(ag, nil, time.Now())
		require.NoError(t, err)
		assert.Equal(t, "p1", streams[0].Values[0].Meta["pod"])
		assert.Equal(t, "fp", streams[0].Values[0].Meta["fingerprint"])
	})
}

func testClient(allowed ...string) *Client {
	return &Client{allowedLabels: allowedLabelSet(allowed)}
}

func TestDataToStream_GroupsByStatusAndUsesReceiveTime(t *testing.T) {
	start := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	received := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	ag := &internal.AlertGroup{
		Version:      "4",
		Receiver:     "r",
		Status:       "firing",
		CommonLabels: map[string]string{"alertname": "X"},
		Alerts: internal.Alerts{
			{Status: "firing", StartsAt: start},
			{Status: "resolved", StartsAt: start, EndsAt: end},
		},
	}

	streams, err := testClient().dataToStream(ag, nil, received)
	require.NoError(t, err)
	require.Len(t, streams, 2, "one stream per status")

	byStatus := map[string]stream{}
	for _, s := range streams {
		byStatus[s.Stream["alert_status"]] = s
	}

	require.Contains(t, byStatus, "firing")
	require.Contains(t, byStatus, "resolved")
	assert.Equal(t, received.UnixNano(), byStatus["firing"].Values[0].At.UnixNano(), "firing entry is stamped at receive time")
	assert.Equal(t, received.UnixNano(), byStatus["resolved"].Values[0].At.UnixNano(), "resolved entry is stamped at receive time")
}

// TestDataToStream_OldAlertUsesReceiveTime is the regression test for the 1.1.0
// rollout failure: an alert that has been firing for months must not be pushed
// at its StartsAt, or Loki rejects it (reject_old_samples / entry too far
// behind) and the alert history is silently lost. The alert's own StartsAt and
// EndsAt stay in the JSON log line, so nothing is lost by stamping receive time.
func TestDataToStream_OldAlertUsesReceiveTime(t *testing.T) {
	start := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC) // ~6 months before receipt
	received := time.Date(2026, 7, 9, 4, 32, 9, 0, time.UTC)

	ag := &internal.AlertGroup{
		Version:      "4",
		Receiver:     "r",
		Status:       "firing",
		CommonLabels: map[string]string{"alertname": "X"},
		Alerts: internal.Alerts{
			{Status: "firing", StartsAt: start},
		},
	}

	streams, err := testClient().dataToStream(ag, nil, received)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	require.Len(t, streams[0].Values, 1)

	entry := streams[0].Values[0]
	assert.Equal(t, received.UnixNano(), entry.At.UnixNano(), "entry timestamp must be the receive time, not StartsAt")

	var line FlattenAlertGroup
	require.NoError(t, json.Unmarshal([]byte(entry.Val), &line))
	assert.True(t, line.Alert.StartsAt.Equal(start), "the alert's real StartsAt must still be queryable in the log line")
}

// TestDataToStream_ZeroReceivedAtFallsBackToNow covers WAL records written by an
// older version, which carry no receivedAt.
func TestDataToStream_ZeroReceivedAtFallsBackToNow(t *testing.T) {
	ag := &internal.AlertGroup{
		Version:      "4",
		Receiver:     "r",
		Status:       "firing",
		CommonLabels: map[string]string{"alertname": "X"},
		Alerts:       internal.Alerts{{Status: "firing"}},
	}

	before := time.Now()
	streams, err := testClient().dataToStream(ag, nil, time.Time{})
	require.NoError(t, err)
	at := streams[0].Values[0].At
	assert.False(t, at.Before(before), "a zero receivedAt must fall back to now")
	assert.False(t, at.After(time.Now()), "a zero receivedAt must fall back to now")
}

func TestDataToStream_EmptyAlertsIsError(t *testing.T) {
	_, err := testClient().dataToStream(&internal.AlertGroup{}, nil, time.Now())
	assert.Error(t, err)
}

func TestEnsureMonotonic(t *testing.T) {
	base := time.Unix(0, 1000)
	t.Run("collisions are nudged to be strictly increasing", func(t *testing.T) {
		got := ensureMonotonic([]row{
			{At: base, Val: "a"},
			{At: base, Val: "b"},
			{At: base, Val: "c"},
		})
		require.Len(t, got, 3)
		assert.Equal(t, int64(1000), got[0].At.UnixNano())
		assert.Equal(t, int64(1001), got[1].At.UnixNano())
		assert.Equal(t, int64(1002), got[2].At.UnixNano())
	})

	t.Run("out-of-order entries are sorted ascending", func(t *testing.T) {
		got := ensureMonotonic([]row{
			{At: time.Unix(0, 3000), Val: "late"},
			{At: time.Unix(0, 1000), Val: "early"},
			{At: time.Unix(0, 2000), Val: "mid"},
		})
		assert.Equal(t, "early", got[0].Val)
		assert.Equal(t, "mid", got[1].Val)
		assert.Equal(t, "late", got[2].Val)
	})

	t.Run("distinct ascending timestamps are untouched", func(t *testing.T) {
		got := ensureMonotonic([]row{
			{At: time.Unix(0, 1000)},
			{At: time.Unix(0, 2000)},
		})
		assert.Equal(t, int64(1000), got[0].At.UnixNano())
		assert.Equal(t, int64(2000), got[1].At.UnixNano())
	})
}

// TestDataToStream_CollidingTimestampsArePreserved is the regression test for
// the silent-drop bug: every alert in a group is stamped at the same receive
// time, so each must be nudged to a unique timestamp for Loki to keep them all.
func TestDataToStream_CollidingTimestampsArePreserved(t *testing.T) {
	start := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	ag := &internal.AlertGroup{
		Version:      "4",
		Receiver:     "r",
		Status:       "firing",
		CommonLabels: map[string]string{"alertname": "X"},
		Alerts: internal.Alerts{
			{Status: "firing", StartsAt: start, Labels: map[string]string{"instance": "a"}},
			{Status: "firing", StartsAt: start, Labels: map[string]string{"instance": "b"}},
			{Status: "firing", StartsAt: start, Labels: map[string]string{"instance": "c"}},
		},
	}

	streams, err := testClient().dataToStream(ag, nil, time.Now())
	require.NoError(t, err)
	require.Len(t, streams, 1)

	seen := map[int64]bool{}
	for _, v := range streams[0].Values {
		ns := v.At.UnixNano()
		assert.Falsef(t, seen[ns], "duplicate timestamp %d would be dropped by Loki", ns)
		seen[ns] = true
	}
	assert.Len(t, seen, 3, "all three colliding alerts must survive")
}

func TestBuildStreamLabels_AllowList(t *testing.T) {
	ag := &internal.AlertGroup{
		Receiver:     "r",
		Status:       "firing",
		CommonLabels: map[string]string{"alertname": "X", "severity": "warning", "secret": "nope"},
	}
	labels := testClient("severity").buildStreamLabels(ag, map[string]string{"extra": "yes"})

	assert.Equal(t, "warning", labels["severity"], "configured label promoted")
	assert.Equal(t, "yes", labels["extra"], "extra label applied")
	assert.Equal(t, "alertsnitch", labels["service_name"])
	assert.Equal(t, "X", labels["alert_name"])
	_, hasSecret := labels["secret"]
	assert.False(t, hasSecret, "non-allowed label excluded")
}

// TestDefaultAllowedLabels_NoHighCardinality guards the cardinality fix: the
// built-in allow-list must not promote labels that explode Loki's active-stream
// count (each distinct value of these is effectively unbounded).
func TestDefaultAllowedLabels_NoHighCardinality(t *testing.T) {
	highCard := map[string]bool{
		"instance": true, "pod": true, "node": true, "container": true,
	}
	for _, l := range defaultAllowedLabels {
		assert.Falsef(t, highCard[l], "high-cardinality label %q must not be a default stream label", l)
	}
}

func TestIsValidLabelName(t *testing.T) {
	valid := []string{"severity", "alert_name", "_x", "Env2", "a1_b2"}
	invalid := []string{"", "1abc", "app-id", "has space", "dot.name", "x/y", "x:y"}
	for _, n := range valid {
		assert.Truef(t, isValidLabelName(n), "%q should be valid", n)
	}
	for _, n := range invalid {
		assert.Falsef(t, isValidLabelName(n), "%q should be invalid", n)
	}
}

// TestBuildStreamLabels_DropsInvalidAndEmpty ensures a query-param label with an
// invalid name can't poison the push, and that empty values are skipped.
func TestBuildStreamLabels_DropsInvalidAndEmpty(t *testing.T) {
	ag := &internal.AlertGroup{
		Receiver:     "r",
		Status:       "firing",
		CommonLabels: map[string]string{"alertname": "X"},
	}
	labels := testClient("severity").buildStreamLabels(ag, map[string]string{
		"app-id": "bad-name", // invalid Loki label name -> dropped
		"region": "",         // empty value -> dropped
		"source": "alertmanager",
	})

	_, hasBad := labels["app-id"]
	assert.False(t, hasBad, "invalid label name must be dropped")
	_, hasEmpty := labels["region"]
	assert.False(t, hasEmpty, "empty value must be dropped")
	assert.Equal(t, "alertmanager", labels["source"], "valid extra label kept")
}

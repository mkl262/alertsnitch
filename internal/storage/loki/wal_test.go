package loki

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mikehsu0618/alertsnitch/internal"
)

func walGroup(receiver string) *internal.AlertGroup {
	return &internal.AlertGroup{
		Receiver:     receiver,
		Status:       "firing",
		CommonLabels: map[string]string{"alertname": "X"},
		Alerts:       internal.Alerts{{Status: "firing"}},
	}
}

func TestWAL_AppendRecoverAck(t *testing.T) {
	dir := t.TempDir()

	w, err := openWAL(dir)
	require.NoError(t, err)
	require.Empty(t, w.recover(), "fresh WAL has nothing to recover")

	s1, err := w.append(walGroup("a"), map[string]string{"src": "am"}, time.Now())
	require.NoError(t, err)
	s2, err := w.append(walGroup("b"), nil, time.Now())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), s1)
	assert.Equal(t, uint64(2), s2)
	require.NoError(t, w.close())

	// Reopen: both records are unacknowledged, so both replay.
	w2, err := openWAL(dir)
	require.NoError(t, err)
	recovered := w2.recover()
	require.Len(t, recovered, 2)
	assert.Equal(t, "a", recovered[0].Group.Receiver)
	assert.Equal(t, "am", recovered[0].ExtraLabels["src"])
	assert.Equal(t, uint64(2), w2.seq, "sequence continues monotonically after reopen")

	// Acknowledge both, then a fresh open recovers nothing.
	require.NoError(t, w2.ack([]uint64{s1, s2}))
	require.NoError(t, w2.close())

	w3, err := openWAL(dir)
	require.NoError(t, err)
	assert.Empty(t, w3.recover(), "acknowledged records must not replay")
	assert.Equal(t, uint64(2), w3.checkpoint)
	require.NoError(t, w3.close())
}

func TestWAL_ContiguousCheckpointOnly(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err := w.append(walGroup("r"), nil, time.Now())
		require.NoError(t, err)
	}

	// Ack 1 and 3 but not 2: the checkpoint may only advance to 1.
	require.NoError(t, w.ack([]uint64{1, 3}))
	assert.Equal(t, uint64(1), w.checkpoint)
	require.NoError(t, w.close())

	w2, err := openWAL(dir)
	require.NoError(t, err)
	recovered := w2.recover()
	// Records 2 and 3 are past the persisted checkpoint of 1, so both replay
	// (at-least-once: 3 was acked in memory but that ack never became durable).
	require.Len(t, recovered, 2)
	assert.Equal(t, uint64(2), recovered[0].Seq)
	assert.Equal(t, uint64(3), recovered[1].Seq)
	require.NoError(t, w2.close())
}

func TestWAL_TruncatedTailIsIgnored(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	require.NoError(t, err)
	_, err = w.append(walGroup("ok"), nil, time.Now())
	require.NoError(t, err)
	require.NoError(t, w.close())

	// Simulate a crash mid-append by appending a garbage partial frame.
	f, err := os.OpenFile(filepath.Join(dir, walLogName), os.O_WRONLY|os.O_APPEND, 0o640)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x00, 0x00, 0x10}) // 3 bytes: an incomplete length prefix
	require.NoError(t, err)
	require.NoError(t, f.Close())

	w2, err := openWAL(dir)
	require.NoError(t, err)
	recovered := w2.recover()
	require.Len(t, recovered, 1, "the one intact record survives; the torn tail is ignored")
	assert.Equal(t, "ok", recovered[0].Group.Receiver)
	require.NoError(t, w2.close())
}

func TestWAL_RecordTooLargeIsRejected(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	require.NoError(t, err)
	defer w.close()

	huge := walGroup("big")
	huge.CommonAnnotations = map[string]string{"description": string(make([]byte, walMaxRecordLen+1))}

	_, err = w.append(huge, nil, time.Now())
	require.Error(t, err, "an oversized record must be rejected, not silently corrupt the log")
	assert.Equal(t, uint64(0), w.seq, "a rejected append must not consume a sequence number")
}

func TestOpenWAL_DirIsAFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	_, err := openWAL(file)
	assert.Error(t, err, "opening a WAL whose dir path is a file must fail")
}

func TestOpenWAL_CorruptCheckpoint(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, walCheckpoint), []byte("not-a-number"), 0o600))

	_, err := openWAL(dir)
	assert.Error(t, err, "a corrupt checkpoint must surface as an error, not a silent reset")
}

func TestNew_WALOpenFailureSurfaces(t *testing.T) {
	fake := newFakeLoki()
	defer fake.close()

	// Point the WAL dir at a regular file so openWAL fails; New must propagate it.
	file := filepath.Join(t.TempDir(), "blocker")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	cfg := testConfig(t, fake.server.URL)
	cfg.Batch = DefaultBatchConfig()
	cfg.Batch.Enabled = true
	cfg.WAL = WALConfig{Enabled: true, Dir: file}

	_, err := New(cfg)
	assert.Error(t, err)
}

// TestWAL_OperationsAfterCloseAreSafe is the regression test for the shutdown
// race where a still-draining flusher could ack (and trigger compaction) after
// the WAL was closed, dereferencing a nil file handle. After close, append and
// ack must return cleanly instead of panicking.
func TestWAL_OperationsAfterCloseAreSafe(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	require.NoError(t, err)
	s1, err := w.append(walGroup("r"), nil, time.Now())
	require.NoError(t, err)
	require.NoError(t, w.close())

	assert.NotPanics(t, func() {
		_, appendErr := w.append(walGroup("r"), nil, time.Now())
		assert.ErrorIs(t, appendErr, errWALClosed, "append after close must report closed, not panic")
		// ack after close must not panic even if it would otherwise compact.
		_ = w.ack([]uint64{s1})
	})
}

func TestWAL_Compaction(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir)
	require.NoError(t, err)

	// Append enough to exceed the compaction threshold, acking all but the last
	// so compaction has something to drop and something to keep.
	const n = 200
	big := walGroup("r")
	big.CommonAnnotations = map[string]string{"description": string(make([]byte, 64*1024))}

	var seqs []uint64
	for i := 0; i < n; i++ {
		s, err := w.append(big, nil, time.Now())
		require.NoError(t, err)
		seqs = append(seqs, s)
	}
	require.Greater(t, w.size, int64(walCompactBytes), "test must actually cross the compaction threshold")

	// Ack all but the final record; the contiguous checkpoint reaches n-1 and the
	// oversized log is rewritten down to the single survivor.
	require.NoError(t, w.ack(seqs[:n-1]))
	assert.Equal(t, uint64(n-1), w.checkpoint)
	assert.LessOrEqual(t, w.size, int64(walCompactBytes), "log must shrink after compaction")
	require.NoError(t, w.close())

	w2, err := openWAL(dir)
	require.NoError(t, err)
	recovered := w2.recover()
	require.Len(t, recovered, 1, "only the unacknowledged record survives compaction")
	assert.Equal(t, uint64(n), recovered[0].Seq)
	require.NoError(t, w2.close())
}

// TestWAL_ReceivedAtSurvivesReplay pins the property the at-least-once guarantee
// rests on: a replayed record must produce the same Loki entry timestamp it had
// on its first push, so Loki drops the byte-identical repeat instead of storing
// the alert twice.
func TestWAL_ReceivedAtSurvivesReplay(t *testing.T) {
	dir := t.TempDir()
	received := time.Date(2026, 7, 9, 4, 32, 9, 123456789, time.UTC)

	w, err := openWAL(dir)
	require.NoError(t, err)
	_, err = w.append(walGroup("a"), nil, received)
	require.NoError(t, err)
	require.NoError(t, w.close())

	w2, err := openWAL(dir)
	require.NoError(t, err)
	defer w2.close()

	recovered := w2.recover()
	require.Len(t, recovered, 1)
	require.True(t, recovered[0].ReceivedAt.Equal(received), "receivedAt must round-trip through the WAL")

	c := testClient()
	first, err := c.dataToStream(walGroup("a"), nil, received)
	require.NoError(t, err)
	replayed, err := c.dataToStream(recovered[0].Group, recovered[0].ExtraLabels, recovered[0].ReceivedAt)
	require.NoError(t, err)
	assert.Equal(t, first[0].Values[0].At.UnixNano(), replayed[0].Values[0].At.UnixNano(),
		"a replayed record must reproduce the original entry timestamp")
	assert.Equal(t, first[0].Values[0].Val, replayed[0].Values[0].Val)
}

// TestWAL_LegacyRecordWithoutReceivedAt covers records written by a version that
// did not persist receivedAt: they must still replay, stamped at push time.
func TestWAL_LegacyRecordWithoutReceivedAt(t *testing.T) {
	dir := t.TempDir()
	f, err := os.OpenFile(filepath.Join(dir, walLogName), os.O_CREATE|os.O_WRONLY, 0o640)
	require.NoError(t, err)
	legacy := []byte(`{"seq":1,"group":{"receiver":"old","status":"firing","alerts":[{"status":"firing"}]}}`)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(legacy)))
	_, err = f.Write(append(lenBuf[:], legacy...))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	w, err := openWAL(dir)
	require.NoError(t, err)
	defer w.close()

	recovered := w.recover()
	require.Len(t, recovered, 1)
	require.True(t, recovered[0].ReceivedAt.IsZero())

	before := time.Now()
	streams, err := testClient().dataToStream(recovered[0].Group, nil, recovered[0].ReceivedAt)
	require.NoError(t, err)
	at := streams[0].Values[0].At
	assert.False(t, at.Before(before), "a legacy record is stamped at replay time")
}

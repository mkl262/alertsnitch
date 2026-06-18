package loki

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/mikehsu0618/alertsnitch/internal"
)

// The WAL gives batch mode crash durability. Without it, alerts that have been
// accepted but not yet flushed live only in an in-memory channel, so a process
// crash (or a shutdown that misses its drain deadline) loses them. With it,
// every accepted alert is appended to disk before it enters the pipeline and is
// only forgotten once its batch reaches a terminal outcome (delivered or
// permanently failed). On startup, records not yet acknowledged are replayed.
//
// Semantics are at-least-once: a crash after a successful push but before the
// checkpoint advances replays already-delivered alerts. Duplicates are tolerable
// here — the timestamp de-collision (timestamps.go) keeps entries unique and
// Loki itself drops byte-identical (timestamp, line) repeats.
const (
	walLogName      = "wal.log"
	walCheckpoint   = "wal.ckpt"
	walMaxRecordLen = 8 << 20 // 8 MiB guard against a corrupt length prefix
	walCompactBytes = 8 << 20 // rewrite the log once it grows past this
)

// errWALClosed is returned when an operation needs the log file but it has been
// closed (during shutdown) or left nil by a failed compaction swap.
var errWALClosed = errors.New("loki wal is closed")

// walRecord is one durably-logged alert awaiting delivery.
type walRecord struct {
	Seq         uint64               `json:"seq"`
	Group       *internal.AlertGroup `json:"group"`
	ExtraLabels map[string]string    `json:"extraLabels,omitempty"`
}

// wal is an append-only write-ahead log with a contiguous-ack checkpoint. It is
// safe for concurrent use: appends come from request goroutines while acks and
// compaction come from the single flusher goroutine.
type wal struct {
	dir string

	mu         sync.Mutex
	f          *os.File
	size       int64
	seq        uint64          // last assigned sequence number
	checkpoint uint64          // highest contiguously-acknowledged sequence
	acked      map[uint64]bool // acks above the checkpoint, awaiting contiguity
	pending    []walRecord     // records to replay, captured at open time
}

// openWAL opens (creating if needed) the WAL in dir, loads the persisted
// checkpoint, and scans the log so the next sequence number continues
// monotonically and unacknowledged records are queued for replay.
func openWAL(dir string) (*wal, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating wal dir %q: %w", dir, err)
	}

	checkpoint, err := readCheckpoint(filepath.Join(dir, walCheckpoint))
	if err != nil {
		return nil, err
	}

	logPath := filepath.Join(dir, walLogName)
	records, size, err := readRecords(logPath)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, fmt.Errorf("opening wal log %q: %w", logPath, err)
	}

	w := &wal{
		dir:        dir,
		f:          f,
		size:       size,
		checkpoint: checkpoint,
		acked:      make(map[uint64]bool),
	}
	for _, rec := range records {
		if rec.Seq > w.seq {
			w.seq = rec.Seq
		}
		if rec.Seq > checkpoint {
			w.pending = append(w.pending, rec)
		}
	}
	return w, nil
}

// recover returns the records that must be replayed (those past the checkpoint)
// and releases the open-time snapshot. It is meant to be called exactly once,
// right after openWAL.
func (w *wal) recover() []walRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.pending
	w.pending = nil
	return out
}

// append durably writes one record and returns its assigned sequence number.
// The data is fsync'd before returning so a crash cannot lose an accepted alert.
func (w *wal) append(group *internal.AlertGroup, extraLabels map[string]string) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.f == nil {
		return 0, errWALClosed
	}

	w.seq++
	rec := walRecord{Seq: w.seq, Group: group, ExtraLabels: extraLabels}
	body, err := json.Marshal(rec)
	if err != nil {
		w.seq-- // nothing was written; keep sequence numbers gap-free
		return 0, fmt.Errorf("marshaling wal record: %w", err)
	}
	if len(body) > walMaxRecordLen {
		w.seq--
		return 0, fmt.Errorf("wal record too large: %d bytes (max %d)", len(body), walMaxRecordLen)
	}

	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(body))) //nolint:gosec // bounded by walMaxRecordLen above
	copy(frame[4:], body)

	if _, err := w.f.Write(frame); err != nil {
		w.seq--
		return 0, fmt.Errorf("writing wal record: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		// The write may have reached the page cache, but durability is not
		// guaranteed, so treat the record as never accepted. Decrementing keeps
		// sequence numbers gap-free; otherwise this orphaned seq would never be
		// acked and the contiguous checkpoint would stall for the whole process.
		w.seq--
		return 0, fmt.Errorf("syncing wal: %w", err)
	}
	w.size += int64(len(frame))
	return w.seq, nil
}

// ack marks the given sequence numbers as terminally handled, advances the
// contiguous checkpoint, persists it, and compacts the log when it has grown
// large. Acks may arrive slightly out of order; only a contiguous run advances
// the durable checkpoint.
func (w *wal) ack(seqs []uint64) error {
	if len(seqs) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, s := range seqs {
		if s > w.checkpoint {
			w.acked[s] = true
		}
	}
	advanced := false
	for w.acked[w.checkpoint+1] {
		w.checkpoint++
		delete(w.acked, w.checkpoint)
		advanced = true
	}
	if !advanced {
		return nil
	}

	if err := writeCheckpoint(filepath.Join(w.dir, walCheckpoint), w.checkpoint); err != nil {
		return err
	}
	if w.size > walCompactBytes {
		if err := w.compactLocked(); err != nil {
			// Compaction is an optimization; a failure must not drop the ack.
			logrus.Errorf("loki wal compaction failed: %v", err)
		}
	}
	return nil
}

// compactLocked rewrites the log keeping only records past the checkpoint. The
// caller must hold w.mu.
func (w *wal) compactLocked() error {
	if w.f == nil {
		return errWALClosed
	}
	logPath := filepath.Join(w.dir, walLogName)
	records, _, err := readRecords(logPath)
	if err != nil {
		return err
	}

	tmpPath := logPath + ".compact"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return fmt.Errorf("creating compaction temp: %w", err)
	}

	var newSize int64
	writer := bufio.NewWriter(tmp)
	for _, rec := range records {
		if rec.Seq <= w.checkpoint {
			continue
		}
		n, werr := writeRecord(writer, rec)
		if werr != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return werr
		}
		newSize += n
	}
	if err := writer.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("flushing compaction temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("syncing compaction temp: %w", err)
	}
	tmp.Close()

	if err := w.f.Close(); err != nil {
		return fmt.Errorf("closing wal before compaction swap: %w", err)
	}
	// Null the handle immediately: if the rename or reopen below fails, append()
	// gets a clear errWALClosed rather than writing to a closed descriptor for
	// the rest of the process lifetime.
	w.f = nil
	if err := os.Rename(tmpPath, logPath); err != nil {
		return fmt.Errorf("swapping compacted wal: %w", err)
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("reopening wal after compaction: %w", err)
	}
	w.f = f
	w.size = newSize
	return nil
}

// close fsyncs and releases the log file handle.
func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	syncErr := w.f.Sync()
	closeErr := w.f.Close()
	w.f = nil
	// Zero the size so a late ack() from a still-draining flusher cannot trip the
	// compaction branch (which would touch the now-nil handle) during a
	// timeout-path shutdown.
	w.size = 0
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// writeRecord encodes one length-prefixed record and returns the bytes written.
func writeRecord(w io.Writer, rec walRecord) (int64, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("marshaling wal record: %w", err)
	}
	if len(body) > walMaxRecordLen {
		return 0, fmt.Errorf("wal record too large: %d bytes (max %d)", len(body), walMaxRecordLen)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body))) //nolint:gosec // bounded by walMaxRecordLen above
	if _, err := w.Write(lenBuf[:]); err != nil {
		return 0, err
	}
	if _, err := w.Write(body); err != nil {
		return 0, err
	}
	return int64(4 + len(body)), nil
}

// readRecords reads every intact record from path, returning them in order
// along with the total bytes consumed. A truncated trailing frame (a crash
// mid-append) is ignored rather than treated as corruption.
func readRecords(path string) ([]walRecord, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("opening wal log %q: %w", path, err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var records []walRecord
	var consumed int64
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // clean end, or a torn length prefix from a crash
			}
			return nil, 0, fmt.Errorf("reading wal length: %w", err)
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		if n == 0 || n > walMaxRecordLen {
			break // corrupt/garbage length: stop at the last good record
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(reader, body); err != nil {
			break // truncated body from a crash mid-write
		}
		var rec walRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			break // unparsable record: stop, keep what we have
		}
		records = append(records, rec)
		consumed += int64(4 + n)
	}
	return records, consumed, nil
}

func readCheckpoint(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("reading wal checkpoint: %w", err)
	}
	var seq uint64
	if _, err := fmt.Sscanf(string(data), "%d", &seq); err != nil {
		return 0, fmt.Errorf("parsing wal checkpoint %q: %w", string(data), err)
	}
	return seq, nil
}

// writeCheckpoint persists the checkpoint atomically via a temp file + rename so
// a crash never leaves a half-written checkpoint.
func writeCheckpoint(path string, seq uint64) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d", seq)), 0o600); err != nil {
		return fmt.Errorf("writing wal checkpoint: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("committing wal checkpoint: %w", err)
	}
	return nil
}

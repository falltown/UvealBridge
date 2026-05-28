// Package wal implements a minimal append-only, fsync-on-append log of
// opaque newline-framed records. It knows nothing about the contents of
// the records it stores — that's the caller's concern.
//
// Typical lifecycle:
//
//	l, err := wal.Open(dir)
//	if err != nil { ... }
//	defer l.Close()
//	if err := l.Replay(func(b []byte) error { ... apply b ... }); err != nil { ... }
//	if err := l.Append([]byte("record bytes")); err != nil { ... }
//
// Replay must be called exactly once, before the first Append. It both
// rebuilds caller state and truncates any partial trailing record left
// over from a previous crash, so future Appends start at a clean offset.
package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const fileName = "wal.log"

// Log is an append-only log backed by a single local file. It is NOT
// safe for concurrent use; the caller must serialize Append calls
// (typically with a mutex). Replay and Close likewise expect a single
// caller.
type Log struct {
	f        *os.File
	w        *bufio.Writer
	replayed bool
}

// Open opens or creates the log file inside dir, which must already exist.
// The returned *Log is positioned for Replay; do not call Append before
// Replay has returned successfully.
func Open(dir string) (*Log, error) {
	path := filepath.Join(dir, fileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}
	return &Log{f: f}, nil
}

// Replay calls fn with each complete (newline-terminated) record in the
// log, in append order. After Replay returns nil, the log is positioned
// at the end of the last complete record, any partial trailing bytes
// (from a crash mid-append) have been truncated, and the log is ready
// for Append.
//
// If fn returns an error, Replay returns it wrapped with the byte offset
// at which the failing record began — useful for diagnosing corruption.
func (l *Log) Replay(fn func(record []byte) error) error {
	if l.replayed {
		return errors.New("wal: Replay called more than once")
	}
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(l.f)
	var consumed int64
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			// Strip trailing newline before handing to the caller; the
			// framing is wal's concern, not theirs.
			if cberr := fn(line[:len(line)-1]); cberr != nil {
				return fmt.Errorf("wal replay at offset %d: %w", consumed, cberr)
			}
			consumed += int64(len(line))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	// Drop any partial trailing record from a previous crash and position
	// the underlying file at the clean offset before wrapping it in a
	// buffered writer.
	if err := l.f.Truncate(consumed); err != nil {
		return err
	}
	if _, err := l.f.Seek(consumed, io.SeekStart); err != nil {
		return err
	}
	l.w = bufio.NewWriter(l.f)
	l.replayed = true
	return nil
}

// Append durably writes record (followed by a newline) to the log.
// record must not itself contain a '\n' byte — the caller chooses an
// encoding (JSON, base64, length-prefix-then-hex, etc.) that satisfies
// this. Append returns only after the bytes have been fsynced to the
// underlying device.
func (l *Log) Append(record []byte) error {
	if !l.replayed {
		return errors.New("wal: Append called before Replay")
	}
	if _, err := l.w.Write(record); err != nil {
		return err
	}
	if err := l.w.WriteByte('\n'); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Close flushes pending writes and closes the underlying file. Safe to
// call multiple times.
func (l *Log) Close() error {
	if l.f == nil {
		return nil
	}
	if l.w != nil {
		if err := l.w.Flush(); err != nil {
			return err
		}
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	err := l.f.Close()
	l.f = nil
	return err
}

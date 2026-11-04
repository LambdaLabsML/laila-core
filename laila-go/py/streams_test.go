package py

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// Regression: ``readexactly(n)`` with ``n > 2 * limit`` must not deadlock.
// asyncio resumes a paused transport inside ``_wait_for_data`` precisely for
// this case; the pump pauses at ``2 * limit`` and would otherwise never be
// woken because nothing consumes until the whole frame is buffered.
func TestStreamReaderReadexactlyBeyondLimit(t *testing.T) {
	pr, pw := io.Pipe()
	reader, _ := Stream_pair(pr, pw, nil, 1024) // pause at 2 KiB
	frame := make([]byte, 256*1024)
	for i := range frame {
		frame[i] = byte(i)
	}
	go func() {
		_, _ = pw.Write(frame)
		_ = pw.Close()
	}()
	done := make(chan []byte, 1)
	go func() {
		got, err := reader.Readexactly(len(frame))
		if err != nil {
			t.Error(err)
		}
		done <- got
	}()
	select {
	case got := <-done:
		if !bytes.Equal(got, frame) {
			t.Fatal("frame corrupted")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("readexactly deadlocked on a frame larger than 2 * limit")
	}
	if rest, err := reader.Read(-1); err != nil || len(rest) != 0 {
		t.Fatalf("trailing read: %v %d", err, len(rest))
	}
}

// Flow control still engages for a *slow* consumer: with more than
// ``2 * limit`` buffered and nobody waiting, the pump blocks in
// Wait_resume_ until the consumer drains the buffer to ``limit``.
func TestStreamReaderPauseResume(t *testing.T) {
	r := New_StreamReader()
	r.Set_limit(10)
	r.Feed_data(make([]byte, 25)) // > 2 * limit -> paused
	paused := make(chan struct{})
	go func() {
		r.Wait_resume_()
		close(paused)
	}()
	select {
	case <-paused:
		t.Fatal("pump was not paused above 2 * limit")
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := r.Read(5); err != nil { // 20 left: still > limit
		t.Fatal(err)
	}
	select {
	case <-paused:
		t.Fatal("pump resumed before the buffer drained to limit")
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := r.Read(10); err != nil { // 10 left: == limit -> resume
		t.Fatal(err)
	}
	select {
	case <-paused:
	case <-time.After(2 * time.Second):
		t.Fatal("pump did not resume once the buffer drained to limit")
	}
}

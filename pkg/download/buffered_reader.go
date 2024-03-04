package download

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// A bufferedReader wraps an http.Response.Body so that it can be eagerly
// downloaded to a buffer before the actual io.Reader consumer can read it.
// It implements io.Reader.
type bufferedReader struct {
	ready bool
	c     sync.Cond
	buf   *bytes.Buffer
	err   error
}

var _ io.Reader = &bufferedReader{}

func newBufferedReader(capacity int64) *bufferedReader {
	return &bufferedReader{
		c:   sync.Cond{L: new(sync.Mutex)},
		buf: bytes.NewBuffer(make([]byte, 0, capacity)),
	}
}

// Read implements io.Reader. It will block until the full body is available for
// reading.
func (b *bufferedReader) Read(buf []byte) (int, error) {
	b.waitOnReady()
	if b.err != nil {
		return 0, b.err
	}
	return b.buf.Read(buf)
}

func (b *bufferedReader) done() {
	b.c.L.Lock()
	defer b.c.L.Unlock()
	b.ready = true
	b.c.Broadcast()
}

func (b *bufferedReader) downloadBody(resp *http.Response) error {
	if b.ready {
		return fmt.Errorf("bufferedReader has already been marked as ready")
	}
	expectedBytes := resp.ContentLength

	if expectedBytes > int64(b.buf.Cap()) {
		b.err = fmt.Errorf("Tried to download 0x%x bytes to a 0x%x-sized buffer", expectedBytes, b.buf.Cap())
		return b.err
	}
	n, err := b.buf.ReadFrom(resp.Body)
	if err != nil && err != io.EOF {
		b.err = fmt.Errorf("error reading response for %s: %w", resp.Request.URL.String(), err)
		return b.err
	}
	if n != expectedBytes {
		b.err = fmt.Errorf("downloaded %d bytes instead of %d for %s", n, expectedBytes, resp.Request.URL.String())
		return b.err
	}
	return nil
}

func (b *bufferedReader) waitOnReady() {
	b.c.L.Lock()
	for !b.ready {
		b.c.Wait()
	}
	b.c.L.Unlock()
}

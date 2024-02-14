package download

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// A bufferedReader wraps an http.Response.Body so that it can be eagerly
// downloaded to a buffer before the actual io.Reader consumer can read it.
// It implements io.Reader.
type bufferedReader struct {
	// ready channel is closed when we're ready to read
	ready   chan struct{}
	started chan struct{}
	buf     *bytes.Buffer
	err     error
	size    int64
}

var _ io.Reader = &bufferedReader{}

func newBufferedReader(capacity int64) *bufferedReader {
	return &bufferedReader{
		ready:   make(chan struct{}),
		started: make(chan struct{}),
		buf:     bytes.NewBuffer(make([]byte, 0, capacity)),
		size:    -1,
	}
}

// Read implements io.Reader. It will block until the full body is available for
// reading.
func (b *bufferedReader) Read(buf []byte) (int, error) {
	<-b.ready
	if b.err != nil {
		return 0, b.err
	}
	return b.buf.Read(buf)
}

func (b *bufferedReader) done() {
	close(b.ready)
}

func (b *bufferedReader) contentLengthReceived() {
	close(b.started)
}

func (b *bufferedReader) downloadBody(resp *http.Response) error {
	expectedBytes := resp.ContentLength
	b.size = expectedBytes
	b.contentLengthReceived()
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

func (b *bufferedReader) len() int64 {
	<-b.started
	return b.size
}

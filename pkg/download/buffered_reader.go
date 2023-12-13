package download

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// A bufferedReader wraps an http.Response.Body so that it can be eagerly
// downloaded to a buffer before the actual io.Reader consumer can read it.
// It implements io.Reader and io.WriterTo (for zero-copy reads).
type bufferedReader struct {
	// ready channel is closed when we're ready to read
	ready chan struct{}
	buf   *bytes.Buffer
	err   error
}

var _ io.Reader = &bufferedReader{}
var _ io.WriterTo = &bufferedReader{}

func newBufferedReader(capacity int64) *bufferedReader {
	return &bufferedReader{
		ready: make(chan struct{}),
		buf:   bytes.NewBuffer(make([]byte, 0, capacity)),
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

// WriteTo implements io.WriterTo. It will block until the full body is
// available for writing to the given io.Writer.
func (b *bufferedReader) WriteTo(w io.Writer) (int64, error) {
	<-b.ready
	if b.err != nil {
		return 0, b.err
	}
	return b.buf.WriteTo(w)
}

func (b *bufferedReader) done() {
	close(b.ready)
}

func (b *bufferedReader) downloadBody(resp *http.Response) error {
	expectedBytes := resp.ContentLength
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

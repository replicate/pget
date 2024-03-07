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
	// ready channel is closed when we're ready to read
	ready chan struct{}
	buf   *bytes.Buffer
	pool  *bufferPool
}

var _ io.Reader = &bufferedReader{}

var emptyBuffer = bytes.NewBuffer(nil)

func newBufferedReader(pool *bufferPool) *bufferedReader {
	return &bufferedReader{
		ready: make(chan struct{}),
		buf:   pool.Get(),
		pool:  pool,
	}
}

// Read implements io.Reader. It will block until the full body is available for
// reading. Once the underlying buffer is fully read, it will be returned to the
// pool.
func (b *bufferedReader) Read(buf []byte) (int, error) {
	<-b.ready
	n, err := b.buf.Read(buf)
	// If we've read all the data,
	if b.buf.Len() == 0 && b.buf != emptyBuffer {
		// return the buffer to the pool
		b.pool.Put(b.buf)
		// and replace our buffer with something that will always return EOF on
		// future reads
		b.buf = emptyBuffer
	}
	return n, err
}

func (b *bufferedReader) done() {
	close(b.ready)
}

func (b *bufferedReader) downloadBody(resp *http.Response) error {
	expectedBytes := resp.ContentLength

	if expectedBytes > int64(b.buf.Cap()) {
		return fmt.Errorf("Tried to download 0x%x bytes to a 0x%x-sized buffer", expectedBytes, b.buf.Cap())
	}
	n, err := b.buf.ReadFrom(resp.Body)
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading response for %s: %w", resp.Request.URL.String(), err)
	}
	if n != expectedBytes {
		return fmt.Errorf("downloaded %d bytes instead of %d for %s", n, expectedBytes, resp.Request.URL.String())
	}
	return nil
}

type bufferPool struct {
	pool sync.Pool
}

func newBufferPool(capacity int64) *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, capacity))
			},
		},
	}
}

func (p *bufferPool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

func (p *bufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	p.pool.Put(buf)
}

package download

import (
	"bufio"
	"io"
	"strings"
	"sync"
)

// A bufferedReader wraps a bufio.Reader so that it can be shared between
// goroutines, with one fetching data from an upstream reader and another
// reading the data.  It implements io.ReaderFrom and io.Reader.  Read() will
// block until Done() is called.
//
// The intended use is: one goroutine calls Read(), which blocks until data is
// ready.  Another calls ReadFrom() and then Done().  The call to Done()
// unblocks the Read() call and allows it to read the data that was fetched by
// ReadFrom().
type bufferedReader struct {
	// ready channel is closed when we're ready to read
	ready chan struct{}
	buf   *bufio.Reader
	pool  *bufferPool
}

var _ io.Reader = &bufferedReader{}
var _ io.ReaderFrom = &bufferedReader{}

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
	if b.buf == nil {
		return 0, io.EOF
	}
	n, err := b.buf.Read(buf)
	// If we've read all the data,
	if b.buf.Buffered() == 0 {
		// return the buffer to the pool
		b.pool.Put(b.buf)
		// and replace our buffer with something that will always return EOF on
		// future reads
		b.buf = nil
	}
	return n, err
}

func (b *bufferedReader) ReadFrom(r io.Reader) (int64, error) {
	b.buf.Reset(r)
	bytes, err := b.buf.Peek(b.buf.Size())
	if err == io.EOF {
		// ReadFrom does not return io.EOF
		err = nil
	}
	return int64(len(bytes)), err
}

func (b *bufferedReader) Done() {
	close(b.ready)
}

type bufferPool struct {
	pool sync.Pool
}

func newBufferPool(size int64) *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() any {
				return bufio.NewReaderSize(nil, int(size))
			},
		},
	}
}

var emptyReader = strings.NewReader("")

// Get returns a bufio.Reader with the correct size, with a blank underlying io.Reader.
func (p *bufferPool) Get() *bufio.Reader {
	br := p.pool.Get().(*bufio.Reader)
	br.Reset(emptyReader)
	return br
}

func (p *bufferPool) Put(buf *bufio.Reader) {
	p.pool.Put(buf)
}

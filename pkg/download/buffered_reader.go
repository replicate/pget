package download

import (
	"bufio"
	"fmt"
	"io"
	"sync"
)

// A bufferedReader wraps a bufio.Reader so that it can be shared between
// goroutines, with one fetching data from an upstream reader and another
// reading the data.  It implements io.ReaderFrom and io.Reader.  Read() will
// block until Done() is called.
//
// The intended use is: one goroutine calls Read(), which blocks until data is
// ready.  Another calls Prefetch() and then Done().  The call to Done()
// unblocks the Read() call and allows it to read the data that was fetched by
// Prefetch().
//
// Note that Prefetch() will allocate a buffer from the shared pool, and Read()
// will return that buffer to the shared pool once it gets to the end of the
// underlying io.Reader.
type bufferedReader struct {
	// ready channel is closed when we're ready to read
	ready chan struct{}
	buf   *bufio.Reader
	pool  *bufferPool
	errs  chan error
}

var _ io.Reader = &bufferedReader{}

var uninitializedReader = bufio.NewReader(nil)

func newBufferedReader(pool *bufferPool) *bufferedReader {
	return &bufferedReader{
		ready: make(chan struct{}),
		buf:   uninitializedReader,
		pool:  pool,
		errs:  make(chan error, 1),
	}
}

// Read implements io.Reader. It will block until the full body is available for
// reading. Once the underlying buffer is fully read, it will be returned to the
// pool.
func (b *bufferedReader) Read(buf []byte) (int, error) {
	<-b.ready
	err := b.readErr()
	if err != nil {
		return 0, err
	}
	if b.buf == uninitializedReader {
		// this happens if the producer calls Done() without calling Prefetch()
		// or recordError().
		// we signal to the consumer that something has gone wrong
		return 0, fmt.Errorf("internal error: uninitialized chunk")
	}
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

func (b *bufferedReader) Prefetch(r io.Reader) int64 {
	b.buf = b.pool.Get(r)
	var bytes []byte
	var err error
	for {
		bytes, err = b.buf.Peek(b.buf.Size())
		if err != io.ErrNoProgress {
			// keep trying until we make progress
			break
		}
	}
	if err != nil && err != io.EOF {
		// ensure we emit this on Read()
		b.recordError(err)
	}
	return int64(len(bytes))
}

func (b *bufferedReader) recordError(err error) {
	// don't block if the error channel is full.
	select {
	case b.errs <- err:
	default:
	}
}

func (b *bufferedReader) readErr() error {
	select {
	case err := <-b.errs:
		return err
	default:
		return nil
	}
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

// Get returns a bufio.Reader with the correct size, wrapping the given io.Reader.
func (p *bufferPool) Get(r io.Reader) *bufio.Reader {
	br := p.pool.Get().(*bufio.Reader)
	br.Reset(r)
	return br
}

func (p *bufferPool) Put(buf *bufio.Reader) {
	p.pool.Put(buf)
}

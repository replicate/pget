package download

import (
	"bytes"
	"io"
)

// A readerPromise represents an io.Reader whose implementation is not yet
// available but will be in the future.  Read() will block until Done() is
// called.
//
// The intended use is: a consumer goroutine calls Read(), which blocks until
// data is ready.  A producer calls Deliver().  These block
// until the consumer has read the provided data or error.
type readerPromise struct {
	// ready channel is closed when we're ready to read
	ready chan struct{}
	// finished channel is closed when we're done reading
	finished chan struct{}
	buf      []byte
	// if reader is non-nil, buf is always the underlying buffer for the reader
	reader *bytes.Reader
	err    error
}

var _ io.Reader = &readerPromise{}

func newReaderPromise() *readerPromise {
	return &readerPromise{
		ready:    make(chan struct{}),
		finished: make(chan struct{}),
	}
}

// Read implements io.Reader. It will block until the full body is available for
// reading. Once the underlying buffer is fully read, it will be returned to the
// pool.
func (b *readerPromise) Read(buf []byte) (int, error) {
	<-b.ready
	if b.err != nil {
		return 0, b.err
	}
	n, err := b.reader.Read(buf)
	// If we've read all the data,
	if err == io.EOF && b.buf != nil {
		// unblock the producer
		close(b.finished)
		b.buf = nil
	}
	return n, err
}

func (b *readerPromise) Deliver(buf []byte, err error) {
	if buf == nil {
		buf = []byte{}
	}
	b.buf = buf
	b.err = err
	b.reader = bytes.NewReader(buf)
	close(b.ready)
	<-b.finished
}

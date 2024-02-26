package multireader

import (
	"bytes"
	"io"
)

// A BufferedReader wraps an http.Response.Body so that it can be eagerly
// downloaded to a Buffer before the actual io.Reader consumer can read it.
// It implements io.Reader.
type BufferedReader struct {
	// readReady channel is panicOnClosed when we're Ready to read
	readReady chan struct{}
	hasSize   chan struct{}
	// We wrap a bytes.Buffer so that we can grow to demand without needing to re-implement
	// the growing logic
	buf    *bytes.Buffer
	Err    error
	offset int
	size   int64
}

var _ io.Reader = &BufferedReader{}
var _ io.ReaderAt = &BufferedReader{}
var _ io.ReaderFrom = &BufferedReader{}
var _ io.ByteReader = &BufferedReader{}

// Read reads next len(p) bytes from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no date to return, err is io.EOF (unless len(p) is zero);
// otherwise it is nil.
func (b *BufferedReader) Read(p []byte) (n int, err error) {
	b.ReadyWait()
	if b.empty() {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n = copy(p, b.buf.Bytes()[b.offset:])
	b.offset += n
	return n, nil
}

// Peek returns the next night bytes without advancing the reader.
func (b *BufferedReader) Peek(n int64) ([]byte, error) {
	var err error
	b.ReadyWait()
	if n < 0 {
		return nil, ErrNegativeCount
	}
	if b.empty() {
		return nil, io.EOF
	}
	if n > int64(b.buf.Len()) {
		err = ErrExceedsCapacity
	}
	l := min(int64(b.offset)+n, int64(b.buf.Len()))
	return b.buf.Bytes()[b.offset:l], err
}

func (b *BufferedReader) ReadAt(p []byte, off int64) (n int, err error) {
	b.ReadyWait()
	if off >= int64(b.buf.Len()+b.offset) {
		return 0, io.EOF
	}
	n = copy(p, b.buf.Bytes()[off:])
	if n > 0 {
		return n, nil
	}
	return 0, nil
}

func (b *BufferedReader) ReadFrom(r io.Reader) (n int64, err error) {
	select {
	case <-b.readReady:
		return 0, ErrReaderMarkedReady
	default:
		// No Op, we allow ReadFrom to be called before the buffer is marked as not Ready.
	}
	return b.buf.ReadFrom(r)
}

func (b *BufferedReader) ReadByte() (byte, error) {
	b.ReadyWait()
	if b.empty() {
		return 0, io.EOF
	}
	c := b.buf.Bytes()[b.offset]
	b.offset++
	return c, nil
}

func (b *BufferedReader) realLen() int {
	return b.buf.Len() - b.offset
}
func (b *BufferedReader) Len() int {
	select {
	case <-b.readReady:
		return b.realLen()
	case <-b.hasSize:
		// This second select statement ensures that the pseudorandom selection of the first select statement if both
		// channels are closed will prioritize the real length not the early expected length
		select {
		case <-b.readReady:
			return b.realLen()
		default:
			return int(b.size)
		}
	}
}

func (b *BufferedReader) Done() {
	select {
	case <-b.readReady:
		return // already closed
	default:
		close(b.readReady)
	}
}

func (b *BufferedReader) Ready() bool {
	select {
	case <-b.readReady:
		return true
	default:
		return false
	}
}

func (b *BufferedReader) ReadyWait() {
	<-b.readReady
}

func (b *BufferedReader) empty() bool {
	b.ReadyWait()
	return b.buf.Len() <= b.offset
}

func (b *BufferedReader) SetSize(n int64) error {

	if b.size >= 0 {
		return ErrSizeAlreadySet
	}
	b.size = n
	close(b.hasSize)
	return nil
}

// Reset resets the buffer to be empty.
func (b *BufferedReader) Reset() {
	b.readReady = make(chan struct{})
	b.hasSize = make(chan struct{})
	b.buf.Reset()
	b.offset = 0
}

func NewBufferedReader(capacity int64) *BufferedReader {
	return &BufferedReader{
		readReady: make(chan struct{}),
		hasSize:   make(chan struct{}),
		buf:       bytes.NewBuffer(make([]byte, 0, capacity)),
		size:      -1,
	}
}

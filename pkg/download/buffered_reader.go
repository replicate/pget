package download

import (
	"bytes"
	"io"
)

type bufferedReader struct {
	// ready channel is closed when we're ready to read
	ready chan struct{}
	buf   *bytes.Buffer
}

var _ io.Reader = &bufferedReader{}
var _ io.Writer = &bufferedReader{}

func newBufferedReader(b *bytes.Buffer) *bufferedReader {
	return &bufferedReader{
		ready: make(chan struct{}),
		buf:   b,
	}
}

func (b *bufferedReader) Write(buf []byte) (int, error) {
	return b.buf.Write(buf)
}

func (b *bufferedReader) Done() {
	close(b.ready)
}

func (b *bufferedReader) Read(buf []byte) (int, error) {
	<-b.ready
	return b.buf.Read(buf)
}

package extract

import (
	"bytes"
	"errors"
	"io"
)

type readPeeker interface {
	io.Reader
	Peek(int) ([]byte, error)
}

var _ io.Reader = &peekReader{}
var _ readPeeker = &peekReader{}

type peekReader struct {
	reader io.Reader
	buffer *bytes.Buffer
}

func (p *peekReader) Read(b []byte) (int, error) {
	if p.buffer != nil {
		if p.buffer.Len() > 0 {
			n, err := p.buffer.Read(b)
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return n, err
		}
	}
	return p.reader.Read(b)
}

func (p *peekReader) Peek(n int) ([]byte, error) {
	return p.peek(n)
}

func (p *peekReader) peek(n int) ([]byte, error) {
	if p.buffer == nil {
		p.buffer = bytes.NewBuffer(make([]byte, 0, n))
	}
	// Read the next n bytes from the reader
	_, err := io.CopyN(p.buffer, p.reader, int64(n))
	if err != nil {
		return p.buffer.Bytes(), err
	}
	return p.buffer.Bytes(), nil
}

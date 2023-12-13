package download

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

type bufferedReader struct {
	// ready channel is closed when we're ready to read
	ready chan struct{}
	buf   *bytes.Buffer
	err   error
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
	if b.err != nil {
		return -1, b.err
	}
	return b.buf.Read(buf)
}

func (b *bufferedReader) downloadBody(resp *http.Response) error {
	defer b.Done()
	expectedBytes := resp.ContentLength
	n, err := io.Copy(b, resp.Body)
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

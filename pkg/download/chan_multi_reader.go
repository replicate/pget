package download

import (
	"io"
	"sync"
)

type chanMultiReader struct {
	ch         <-chan *bufferedReader
	cur        *bufferedReader
	bufferPool *sync.Pool
}

var _ io.Reader = &chanMultiReader{}

func newChanMultiReader(ch <-chan *bufferedReader, chunkSize int64) *chanMultiReader {
	pool := &sync.Pool{
		New: func() any {
			return newBufferedReader(chunkSize)
		},
	}

	return &chanMultiReader{ch: ch, bufferPool: pool}
}

func (c *chanMultiReader) Read(p []byte) (n int, err error) {
	for {
		if c.cur == nil {
			var ok bool
			c.cur, ok = <-c.ch
			if !ok {
				// no more readers; return EOF
				return 0, io.EOF
			}
		}
		n, err = c.cur.Read(p)
		if err == io.EOF {
			c.putBufferedReader(c.cur)
			c.cur = nil
		}
		if n > 0 || err != io.EOF {
			// we either made progress or hit an error, return to the caller
			if err == io.EOF {
				// TODO: we could eagerly check to see if the channel is closed
				// and return EOF one call early
				err = nil
			}
			return
		}
		// n == 0, err == EOF; this reader is done and we need to start the next
		c.putBufferedReader(c.cur)
		c.cur = nil
	}
}

func (c *chanMultiReader) getBufferedReader() *bufferedReader {
	return c.bufferPool.Get().(*bufferedReader)
}

func (c *chanMultiReader) putBufferedReader(br *bufferedReader) {
	if br == nil {
		return
	}
	br.reset()
	c.bufferPool.Put(br)
}

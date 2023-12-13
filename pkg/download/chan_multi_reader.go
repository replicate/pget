package download

import "io"

type chanMultiReader struct {
	ch  <-chan io.Reader
	cur io.Reader
}

func newChanMultiReader(ch <-chan io.Reader) *chanMultiReader {
	return &chanMultiReader{ch: ch}
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
		c.cur = nil
	}
}

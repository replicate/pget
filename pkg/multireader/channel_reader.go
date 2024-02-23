package multireader

import (
	"errors"
	"io"
)

type MultiBufferedReader struct {
	ch      <-chan *BufferedReader
	current int
	readers []*BufferedReader
	closed  bool
}

var (
	_ io.Reader     = &MultiBufferedReader{}
	_ io.ReaderAt   = &MultiBufferedReader{}
	_ io.ByteReader = &MultiBufferedReader{}
)

var (
	errNoMoreReaders = errors.New("multireader.MultiBufferedReader: no more readers")
)

func NewMultiReader(ch <-chan *BufferedReader) *MultiBufferedReader {
	return &MultiBufferedReader{ch: ch, current: -1}
}

func (mbr *MultiBufferedReader) getNextNonEmptyReader() (*BufferedReader, error) {
	if err := mbr.errIfClosed(); err != nil {
		return nil, err
	}
	for {
		reader, err := mbr.curReader()
		if err != nil {
			return nil, err
		}
		if !reader.empty() {
			return reader, nil
		}
		_, err = mbr.nextReader()
		if err != nil {
			return nil, err
		}
	}
}

func (mbr *MultiBufferedReader) getReader() (*BufferedReader, error) {
	reader, err := mbr.getNextNonEmptyReader()
	if err != nil {
		return nil, mbr.handleReaderErrors(err)
	}
	return reader, nil
}

func (mbr *MultiBufferedReader) handleReaderErrors(err error) error {
	if errors.Is(err, errNoMoreReaders) {
		return io.EOF
	}
	return err
}

// Read reads up to len(p) bytes into p. It returns the number of bytes read (0 <= n <= len(p))
func (mbr *MultiBufferedReader) Read(p []byte) (n int, err error) {
	if err := mbr.errIfClosed(); err != nil {
		return 0, err
	}

	for n < len(p) {
		reader, err := mbr.getReader()
		if err != nil {
			return n, mbr.handleReaderErrors(err)
		}

		bytesRead, err := reader.Read(p[n:])
		n += bytesRead
		if err != nil && !errors.Is(err, io.EOF) {
			return n, err
		}
	}
	return n, nil
}

func (mbr *MultiBufferedReader) getAllReaders() {
	for {
		if err := mbr.getNextReaderFromChannel(); err != nil {
			return
		}
	}
}

func (mbr *MultiBufferedReader) ReadAt(p []byte, off int64) (n int, err error) {
	var totalBytes int64
	if err := mbr.errIfClosed(); err != nil {
		return 0, err
	}
	mbr.getAllReaders()
	for _, reader := range mbr.readers {
		totalBytes += int64(reader.Len()) // 20
		if off < totalBytes {
			innerOffset := off - (totalBytes - int64(reader.Len()))
			return reader.ReadAt(p, innerOffset)
		}
	}
	return 0, io.EOF
}

func (mbr *MultiBufferedReader) ReadByte() (byte, error) {
	if err := mbr.errIfClosed(); err != nil {
		return 0, err
	}
	reader, err := mbr.getReader()
	if err != nil {
		return 0, mbr.handleReaderErrors(err)
	}
	return reader.ReadByte()
}

func (mbr *MultiBufferedReader) nextReader() (*BufferedReader, error) {
	cur := mbr.current + 1
	if cur >= len(mbr.readers) {
		if err := mbr.getNextReaderFromChannel(); err != nil {
			return nil, err
		}
		mbr.current = cur
	}
	return mbr.readers[cur], nil
}

func (mbr *MultiBufferedReader) curReader() (*BufferedReader, error) {
	if mbr.current == -1 || mbr.current >= len(mbr.readers) {
		return mbr.nextReader()
	}
	return mbr.readers[mbr.current], nil
}

func (mbr *MultiBufferedReader) getNextReaderFromChannel() error {
	reader, ok := <-mbr.ch
	if !ok {
		return errNoMoreReaders
	}
	mbr.readers = append(mbr.readers, reader)
	return nil
}

func (mbr *MultiBufferedReader) Close() error {
	if err := mbr.errIfClosed(); err != nil {
		return err
	}
	mbr.readers = nil
	return nil
}

func (mbr *MultiBufferedReader) Closed() bool {
	return mbr.closed
}

func (mbr *MultiBufferedReader) errIfClosed() error {
	if mbr.closed {
		return ErrClosed
	}
	return nil
}

func (mbr *MultiBufferedReader) Peek(n int64) ([]byte, error) {
	if err := mbr.errIfClosed(); err != nil {
		return nil, err
	}

	if n < 0 {
		return nil, ErrNegativeCount
	}

	data := make([]byte, n)
	var bytesRead int64

	readerIdx := max(mbr.current, 0)
	for {

		if len(mbr.readers) == 0 || readerIdx > len(mbr.readers)-1 {
			err := mbr.getNextReaderFromChannel()
			if err != nil {
				// no more readers on the channel and channel is closed
				return data[:bytesRead], io.EOF
			}
		}
		reader := mbr.readers[readerIdx]
		// after we capture the current reader, increment the index
		readerIdx++
		if reader.empty() {
			continue
		}
		peeked, err := reader.Peek(n - bytesRead)
		if errors.Is(err, ErrExceedsCapacity) {
			bytesRead += int64(copy(data[bytesRead:], peeked))
			continue
		}
		if err != nil {
			return nil, err
		}

		bytesRead += int64(copy(data[bytesRead:], peeked))
		return data[:bytesRead], nil
	}
}

func (mbr *MultiBufferedReader) Len() (n int) {
	if err := mbr.errIfClosed(); err != nil {
		return 0
	}

	mbr.getAllReaders()
	for _, reader := range mbr.readers {
		n += reader.Len()
	}
	return n
}

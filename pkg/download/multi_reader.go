package download

import (
	"errors"
	"io"
)

var (
	ErrInvalidOffset = errors.New("download.multiReader: Negative offset")
)

var _ io.ReaderAt = &multiReader{}

type multiReader struct {
	readers []*bufferedReader
}

func NewMultiReader(reader io.Reader) (io.ReaderAt, error) {
	chanMultiReader, ok := reader.(*chanMultiReader)
	if !ok {
		// future may support converting a standard reader into a multi reader with a single reader
		// for now, we only support chanMultiReader
		return nil, errors.New("reader is not a chanMultiReader")
	}
	multiReader := &multiReader{
		readers: make([]*bufferedReader, 0),
	}
	for {
		reader, ok := <-chanMultiReader.ch
		if !ok {
			break
		}
		bufferedReader, ok := reader.(*bufferedReader)
		if !ok {
			// future may support converting a standard reader into a bufferedReader,
			// for now we only support bufferedReader
			return nil, errors.New("reader is not a bufferedReader")
		}
		multiReader.readers = append(multiReader.readers, bufferedReader)
	}
	return multiReader, nil
}

func (m *multiReader) ReadAt(p []byte, off int64) (n int, err error) {
	var readerBytes int64
	if off < 0 {
		return 0, ErrInvalidOffset
	}
	for i, r := range m.readers {
		readerBytes += int64(r.len())
		// if offset is less than the bytes found in the reader slice to this point,
		// we can start reading from this reader.
		if off < readerBytes {
			// Calculate the offset within the reader
			innerOffset := off - (readerBytes - int64(r.len()))
			n = copy(p, r.buf.Bytes()[innerOffset:])
			if i == len(m.readers)-1 && n < len(p) {
				// We are at the last reader and the buffer is not full
				// We need to return io.EOF
				return n, io.EOF
			}
			return n, nil
		}
	}
	// If we are here, we have run through all the possible readers and the offset puts us past the end of the last
	// reader, meaning we should return 0 and io.EOF to indicate there is nothing to read.
	return 0, io.EOF
}

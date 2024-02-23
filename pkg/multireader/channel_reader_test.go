package multireader_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/replicate/pget/pkg/multireader"
)

func generateTestContent(size int64) []byte {
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(rand.Intn(256))
	}
	return content
}

func testBufferedReaders(t *testing.T, count int, readerSize int64, contentSize int64) (readers []*multireader.BufferedReader, content []byte) {
	content = generateTestContent(contentSize)
	for i := 0; i < count; i++ {
		start := int64(i) * readerSize
		end := start + readerSize
		if end > contentSize {
			end = contentSize
		}
		reader := multireader.NewBufferedReader(int64(len(content[start:end])))
		_, _ = reader.ReadFrom(bytes.NewReader(content[start:end]))
		reader.Done()
		readers = append(readers, reader)
	}
	return readers, content

}

func getTestMultiBufferedReader(t *testing.T) (mbr *multireader.MultiBufferedReader, content []byte) {
	readers, content := testBufferedReaders(t, 3, 10, 30)
	ch := make(chan *multireader.BufferedReader, 3)
	mbr = multireader.NewMultiReader(ch)
	for _, reader := range readers {
		ch <- reader
	}
	close(ch)
	return mbr, content
}

func TestNewMultiReader(t *testing.T) {
}

func TestMultiBufferedReader_Peek(t *testing.T) {
	mbr, content := getTestMultiBufferedReader(t)
	tc := []struct {
		name          string
		peekSize      int
		expectedError error
	}{
		{
			name:     "peek 5 less than single reader size",
			peekSize: 5,
		},
		{
			name:     "peek more than single reader size, less than total content size",
			peekSize: 15,
		},
		{
			name:          "peek negative",
			peekSize:      -1,
			expectedError: multireader.ErrNegativeCount,
		},
		{
			name:     "peek 0",
			peekSize: 0,
		},
		{
			name:     "peek content size",
			peekSize: len(content),
		},
		{
			name:          "peek > content size",
			peekSize:      len(content) + 1,
			expectedError: io.EOF,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			peeked, err := mbr.Peek(int64(tt.peekSize))
			if !assert.Equal(t, tt.expectedError, err) {
				peekSize := min(tt.peekSize, len(content))
				require.Equal(t, content[:peekSize], peeked)
				// ensure that the reader has not advanced
				assert.Equal(t, len(content), mbr.Len())
			}
		})
	}
}

func TestMultiBufferedReader_Read(t *testing.T) {
	tc := []struct {
		name          string
		readSize      int
		expectedError error
	}{
		{
			name:     "read 5 less than single reader size",
			readSize: 5,
		},
		{
			name:     "read more than single reader size, less than total content size",
			readSize: 15,
		},
		{
			name:          "read more than total content size",
			readSize:      35,
			expectedError: io.EOF,
		},
		{
			name:     "read 0",
			readSize: 0,
		},
		{
			name:     "read entire content size",
			readSize: 30,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			mbr, content := getTestMultiBufferedReader(t)
			bytesRead := 0
			p := make([]byte, tt.readSize)
			for bytesRead < tt.readSize {
				var n int
				n, err = mbr.Read(p[bytesRead:])
				bytesRead += n
				if err != nil || n == tt.readSize {
					break
				}
			}
			assert.Equal(t, tt.expectedError, err)
			if tt.expectedError != nil {
				assert.Equal(t, len(content), bytesRead)
				assert.Equal(t, content, p[:bytesRead])
			} else {
				assert.Equal(t, tt.readSize, bytesRead)
				assert.Equal(t, content[:tt.readSize], p)
			}
		})

	}
}

func TestMultiBufferedReader_ReadAt(t *testing.T) {
	tc := []struct {
		name             string
		readSize         int
		readOffset       int64
		expectedReadSize int
		expectedError    error
	}{
		{
			name:             "read less than single reader, zero offset",
			readSize:         5,
			readOffset:       0,
			expectedReadSize: 5,
		},
		{
			name:             "read more than single reader, zero offset",
			readSize:         15,
			readOffset:       0,
			expectedReadSize: 10,
		},
		{
			name:             "read more than single reader, non-zero offset",
			readSize:         15,
			readOffset:       5,
			expectedReadSize: 5,
		},
		{
			name:             "offset exceeds total content size",
			readSize:         5,
			readOffset:       35,
			expectedReadSize: 0,
			expectedError:    io.EOF,
		},
		{
			name:             "read less than single reader, offset exceeds single reader",
			readSize:         5,
			readOffset:       15,
			expectedReadSize: 5,
		},
		{
			name:             "read 0, zero offset",
			readSize:         0,
			readOffset:       0,
			expectedReadSize: 0,
		},
		{
			name:             "read 0, non-zero offset",
			readSize:         0,
			readOffset:       5,
			expectedReadSize: 0,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			mbr, content := getTestMultiBufferedReader(t)
			p := make([]byte, tt.readSize)
			n, err := mbr.ReadAt(p, tt.readOffset)
			assert.Equal(t, tt.expectedError, err)
			assert.Equal(t, tt.expectedReadSize, n)
			if tt.expectedReadSize == 0 {
				assert.Equal(t, 0, n)
				assert.Equal(t, make([]byte, tt.readSize), p)
			} else {
				assert.Equal(t, content[tt.readOffset:tt.readOffset+int64(tt.expectedReadSize)], p[:n])
			}
			// Reader should not be advanced
			assert.Equal(t, len(content), mbr.Len())
		})

	}
}

func TestMultiBufferedReader_ReadByte(t *testing.T) {
	mbr, content := getTestMultiBufferedReader(t)
	for _, b := range content {
		read, err := mbr.ReadByte()
		require.NoError(t, err)
		assert.Equal(t, b, read)
	}
	_, err := mbr.ReadByte()
	assert.Equal(t, io.EOF, err)
}

func TestMultiBufferedReader_Len(t *testing.T) {
	mbr, content := getTestMultiBufferedReader(t)
	assert.Equal(t, len(content), mbr.Len())
	// Read some content
	p := make([]byte, 10)
	_, _ = mbr.Read(p)
	assert.Equal(t, len(content)-10, mbr.Len())
}

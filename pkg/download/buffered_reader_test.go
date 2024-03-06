package download

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBufferedReader(t *testing.T) {
	const capacity = int64(100)
	rp := newBufferPool(capacity)
	br := newBufferedReader(rp)
	require.NotNil(t, br)
	assert.Equal(t, capacity, int64(br.buf.Cap()))
	assert.Equal(t, int64(0), int64(br.buf.Len()))
	assert.Equal(t, false, bufferedReadeIsReady(t, br))
	assert.Equal(t, rp, br.pool)
}

func TestBufferedReader_downloadBody(t *testing.T) {
	const dataLen = 1000
	tc := []struct {
		name        string
		bufCap      int64
		httpResp    *http.Response
		expectedErr error
	}{
		{
			name:     "Download body with no error",
			httpResp: &http.Response{ContentLength: int64(dataLen), Body: io.NopCloser(bytes.NewReader([]byte(strings.Repeat("a", dataLen))))},
			bufCap:   dataLen,
		},
		{
			name:     "Download body with no error, less than buffer",
			httpResp: &http.Response{ContentLength: int64(dataLen - 1), Body: io.NopCloser(bytes.NewReader([]byte(strings.Repeat("a", dataLen-1))))},
			bufCap:   dataLen,
		},
		{
			name:        "Download body exceeds buffer",
			httpResp:    &http.Response{ContentLength: int64(dataLen), Body: io.NopCloser(bytes.NewReader([]byte(strings.Repeat("a", dataLen))))},
			bufCap:      dataLen - 1,
			expectedErr: errContentLengthMismatch,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			pool := newBufferPool(tt.bufCap)
			br := newBufferedReader(pool)
			require.NotNil(t, br)
			err := br.downloadBody(tt.httpResp)
			if tt.expectedErr == nil {
				assert.NoError(t, err)
				assert.Equal(t, tt.httpResp.ContentLength, int64(br.buf.Len()))
			}
			assert.ErrorIs(t, err, tt.expectedErr)
		})
	}
}

func TestBufferedReader_readFrom(t *testing.T) {
	reader := bytes.NewReader([]byte(strings.Repeat("a", 1000)))
	reader2 := bytes.NewReader([]byte(strings.Repeat("b", 1000)))
	pool := newBufferPool(1000)
	br := newBufferedReader(pool)
	require.NotNil(t, br)
	n, err := br.readFrom(reader)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), n)
	assert.Equal(t, int64(1000), int64(br.buf.Len()))
	br.done()
	readerBuf := make([]byte, 1000)
	readN, err := br.Read(readerBuf)
	assert.NoError(t, err)
	assert.Equal(t, 1000, readN)
	assert.Equal(t, 0, br.buf.Len())
	defer func() {
		if r := recover(); r == nil {
			assert.Fail(t, "readFrom did not panic with emptyBuffer")
		}
	}()
	_, _ = br.readFrom(reader2)

}

func TestBufferedReader_Read(t *testing.T) {
	testErr := errors.New("error")
	tc := []struct {
		name         string
		expectedErr  error
		expectedRead int
		bufferErr    error
	}{
		{
			name:         "Read with no error",
			expectedErr:  nil,
			expectedRead: 10,
		},
		{
			name:         "Read with error EOF",
			expectedErr:  io.EOF,
			expectedRead: 0,
		},
		{
			name:         "Read content with no error",
			expectedErr:  nil,
			expectedRead: 10,
		},
		{
			name:         "Read non EOF Error",
			expectedErr:  testErr,
			expectedRead: 0,
			bufferErr:    testErr,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			testCase := tt
			wg := new(sync.WaitGroup)
			wg.Add(1)
			pool := newBufferPool(100)
			br := newBufferedReader(pool)
			if testCase.bufferErr != nil {
				br.err = testCase.bufferErr
			}
			if testCase.expectedRead > 0 {
				content := []byte(strings.Repeat("a", 100))
				_, _ = br.buf.ReadFrom(bytes.NewReader(content))
			}
			require.NotNil(t, br)
			assert.False(t, bufferedReadeIsReady(t, br))
			readBuf := make([]byte, 10)
			go func() {
				defer wg.Done()
				n, err := br.Read(readBuf)
				assert.Equal(t, testCase.expectedRead, n)
				assert.Equal(t, testCase.expectedErr, err)
			}()
			br.done()
			wg.Wait()
		},
		)
	}
}

func TestBufferedReader_done(t *testing.T) {
	pool, _ := getReaderPool(t)
	br := newBufferedReader(pool)

	assert.False(t, bufferedReadeIsReady(t, br))
	br.done()
	assert.True(t, bufferedReadeIsReady(t, br))
}

func bufferedReadeIsReady(t *testing.T, br *bufferedReader) bool {
	require.NotNil(t, br)
	select {
	case <-br.ready:
		return true
	default:
		return false
	}
}

func getReaderPool(t *testing.T) (*bufferPool, int64) {
	capacity := 750 + rand.Int63n(2000-750+1)
	rp := newBufferPool(capacity)
	require.NotNil(t, rp)
	return rp, capacity
}

func TestBufferPool_Get(t *testing.T) {
	rp, capacity := getReaderPool(t)
	buf := rp.Get()
	require.NotNil(t, buf)
	assert.Equal(t, capacity, int64(buf.Cap()))
	assert.Equal(t, int64(0), int64(buf.Len()))
}

func TestBufferedReader_Read_EOF(t *testing.T) {
	rp, capacity := getReaderPool(t)
	data := []byte("The quick brown fox jumps over the lazy dog.")
	// Get a buffer from the pool and fill it with data
	buf := newBufferedReader(rp)
	bytesBuffer := buf.buf
	require.NotNil(t, buf)
	_, _ = buf.buf.ReadFrom(bytes.NewReader(data))
	assert.Equal(t, int64(len(data)), int64(buf.buf.Len()))
	// Mark the buffer as ready
	buf.done()
	// Read to EOF, verify the buffer is returned to the pool
	readBuf := make([]byte, capacity+1)
	n, err := buf.Read(readBuf)
	assert.Equal(t, len(data), n)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), int64(buf.buf.Len()))
	assert.Equal(t, buf.buf, emptyBuffer)
	newBytesBuffer := rp.Get()
	assert.Equal(t, &bytesBuffer, &newBytesBuffer)
}

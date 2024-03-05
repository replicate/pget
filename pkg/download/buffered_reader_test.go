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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewBufferedReader(t *testing.T) {
	const capacity = int64(100)
	rp := newReaderPool(capacity)
	br := newBufferedReader(capacity, rp)
	require.NotNil(t, br)
	assert.Equal(t, capacity, int64(br.buf.Cap()))
	assert.Equal(t, int64(0), int64(br.buf.Len()))
	assert.Equal(t, false, br.ready)
	assert.Equal(t, rp, br.pool)
}

func TestBufferedReader_downloadBody(t *testing.T) {
	br := newBufferedReader(100, nil)
	require.NotNil(t, br)
	data := []byte("The quick brown fox jumps over the lazy dog.")
	resp := &http.Response{ContentLength: int64(len(data)), Body: io.NopCloser(bytes.NewReader(data))}
	err := br.downloadBody(resp)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(data)), int64(br.buf.Len()))
	br.done()
	resp = &http.Response{ContentLength: int64(len(data)), Body: io.NopCloser(bytes.NewReader(data))}
	err = br.downloadBody(resp)
	require.Error(t, err)
}

func TestBufferedReader_Read(t *testing.T) {
	testErr := errors.New("error")
	tc := []struct {
		name         string
		expectedErr  error
		expectedRead int
		bufferErr    error
		waitOnReady  bool
	}{
		{
			name:         "Read with no error",
			expectedErr:  nil,
			expectedRead: 10,
			waitOnReady:  false,
		},
		{
			name:         "Read with error EOF",
			expectedErr:  io.EOF,
			expectedRead: 0,
			waitOnReady:  false,
		},
		{
			name:         "Read waiting on ready",
			expectedErr:  nil,
			expectedRead: 10,
			waitOnReady:  true,
		},
		{
			name:         "Read waiting on ready",
			expectedErr:  testErr,
			expectedRead: 0,
			bufferErr:    testErr,
			waitOnReady:  true,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			testCase := tt
			wg := new(sync.WaitGroup)
			wg.Add(1)
			br := newBufferedReader(100, nil)
			if testCase.bufferErr != nil {
				br.err = testCase.bufferErr
			}
			if testCase.expectedRead > 0 {
				content := []byte(strings.Repeat("a", 100))
				_, _ = br.buf.ReadFrom(bytes.NewReader(content))
			}
			require.NotNil(t, br)
			if !tt.waitOnReady {
				br.ready = true
			}
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
	br := newBufferedReader(100, nil)
	assert.False(t, br.ready)
	br.done()
	assert.True(t, br.ready)
}

func getReaderPool(t *testing.T) (*readerPool, int64) {
	capacity := 1000 + rand.Int63n(2225-1000+1)
	rp := newReaderPool(capacity)
	require.NotNil(t, rp)
	return rp, capacity
}

func TestReaderPool_Get(t *testing.T) {
	rp, capacity := getReaderPool(t)
	buf := rp.Get()
	require.NotNil(t, buf)
	assert.Equal(t, capacity, int64(buf.buf.Cap()))
	assert.Equal(t, int64(0), int64(buf.buf.Len()))
	assert.Equal(t, false, buf.ready)

	rp.Put(buf)
	buf.pool = nil

	newBuf := rp.Get()
	require.NotNil(t, buf)
	assert.Equal(t, &buf, &newBuf)

	buf.ready = true
	rp.pool.Put(buf)
	newBuf = rp.Get()
	require.NotNil(t, newBuf)
	assert.NotEqual(t, &buf, &newBuf)
}

func TestReaderPool_Put(t *testing.T) {
	rp, _ := getReaderPool(t)
	// Get a buffer from the pool and fill it with data
	buf := rp.pool.Get().(*bufferedReader)
	require.NotNil(t, buf)
	rp.Put(buf)
	// Get a new buffer from the pool and verify it is the same as the one we just put back
	newBuffer := rp.pool.Get().(*bufferedReader)
	require.NotNil(t, newBuffer)
	assert.Equal(t, &newBuffer, &buf)
	// check a nil put
	rp.Put(nil)
	reader := rp.pool.Get()
	require.NotNil(t, reader)
	assert.NotEqual(t, &newBuffer, &reader)
}

func TestNewReaderPool(t *testing.T) {
	rp, capacity := getReaderPool(t)
	buf := rp.pool.Get().(*bufferedReader)
	require.NotNil(t, buf)
	assert.Equal(t, capacity, int64(buf.buf.Cap()))
	assert.Equal(t, int64(0), int64(buf.buf.Len()))
	assert.Equal(t, false, buf.ready)
}

type mockPool struct {
	mock.Mock
	br *bufferedReader
}

func (m *mockPool) Get() *bufferedReader {
	m.Called()
	return m.br
}

func (m *mockPool) Put(br *bufferedReader) {
	m.Called(br)
}

func TestBufferedReader_Close(t *testing.T) {
	var rp Pool
	mp := &mockPool{}
	rp = mp
	mp.br = newBufferedReader(1024, rp)
	capacity := int64(1024)

	mp.On("Get").Return(mp.br)
	mp.On("Put", mp.br).Return()

	buf := rp.Get()
	require.NotNil(t, buf)
	content := []byte(strings.Repeat("a", 100))
	_, _ = buf.buf.ReadFrom(bytes.NewReader(content))
	buf.done()
	assert.True(t, buf.ready)
	assert.Nil(t, buf.err)
	assert.Equal(t, int64(100), int64(buf.buf.Len()))
	assert.Equal(t, &rp, &buf.pool)
	assert.Equal(t, capacity, int64(buf.buf.Cap()))
	buf.Close()
	assert.NotNil(t, buf.pool)
	assert.Nil(t, buf.err)
	assert.Zero(t, buf.buf.Len())
	assert.Equal(t, capacity, int64(buf.buf.Cap()))
}

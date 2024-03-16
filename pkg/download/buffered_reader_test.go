package download

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferedReaderSerial(t *testing.T) {
	pool := newBufferPool(10)
	br := newBufferedReader(pool)
	n := br.Prefetch(strings.NewReader("foobar"))
	assert.Equal(t, int64(6), n)
	br.Done()
	buf, err := io.ReadAll(br)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", string(buf))
}

func TestBufferedReaderParallel(t *testing.T) {
	pool := newBufferPool(10)
	br := newBufferedReader(pool)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer br.Done()
		defer wg.Done()
		n := br.Prefetch(strings.NewReader("foobar"))
		assert.Equal(t, int64(6), n)
	}()
	buf, err := io.ReadAll(br)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", string(buf))
	wg.Wait()
}

func TestBufferedReaderReadsWholeChunk(t *testing.T) {
	chunkSize := int64(1024 * 1024)
	pool := newBufferPool(chunkSize)
	br := newBufferedReader(pool)
	data := bytes.Repeat([]byte("x"), int(chunkSize))
	n64 := br.Prefetch(bytes.NewReader(data))
	assert.Equal(t, chunkSize, n64)
	br.Done()
	buf := make([]byte, chunkSize)
	// We should only require a single Read() call because all the data should
	// be buffered
	n, err := br.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, data, buf)
	assert.Equal(t, int(chunkSize), n)
}

type alwaysErrorReader struct{ err error }

var _ io.Reader = &alwaysErrorReader{}

func (err *alwaysErrorReader) Read([]byte) (int, error) {
	return 0, err.err
}

func alwaysError(err error) io.Reader { return &alwaysErrorReader{err: err} }

func TestBufferedReaderPrefetchPassesErrorsToConsumer(t *testing.T) {
	pool := newBufferPool(10)
	br := newBufferedReader(pool)

	expectedErr := fmt.Errorf("oh no")

	n64 := br.Prefetch(alwaysError(expectedErr))
	assert.Equal(t, int64(0), n64)
	br.Done()
	buf := make([]byte, 10)
	n, err := br.Read(buf)
	assert.ErrorIs(t, expectedErr, err)
	assert.Equal(t, 0, n)
}

func TestBufferedReaderSubsequentReadsReturnEOF(t *testing.T) {
	pool := newBufferPool(10)
	br := newBufferedReader(pool)
	n64 := br.Prefetch(strings.NewReader("foobar"))
	assert.Equal(t, int64(6), n64)
	br.Done()
	buf, err := io.ReadAll(br)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", string(buf))

	n, err := br.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, io.EOF)
}

func TestBufferedReaderDoneWithoutPrefetch(t *testing.T) {
	pool := newBufferPool(10)
	br := newBufferedReader(pool)
	br.Done()
	_, err := io.ReadAll(br)
	assert.Error(t, err)
}

package download

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferedReaderSerial(t *testing.T) {
	pool := newBufferPool(10)
	br := newBufferedReader(pool)
	n, err := br.ReadFrom(strings.NewReader("foobar"))
	assert.NoError(t, err)
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
		n, err := br.ReadFrom(strings.NewReader("foobar"))
		assert.NoError(t, err)
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
	n64, err := br.ReadFrom(bytes.NewReader(data))
	assert.NoError(t, err)
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

func TestBufferedReaderSubsequentReadsReturnEOF(t *testing.T) {
	pool := newBufferPool(10)
	br := newBufferedReader(pool)
	n64, err := br.ReadFrom(strings.NewReader("foobar"))
	assert.NoError(t, err)
	assert.Equal(t, int64(6), n64)
	br.Done()
	buf, err := io.ReadAll(br)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", string(buf))

	n, err := br.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, io.EOF)
}

func TestBufferedReaderDoneWithoutReadFrom(t *testing.T) {
	pool := newBufferPool(10)
	br := newBufferedReader(pool)
	br.Done()
	buf, err := io.ReadAll(br)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(buf))
}

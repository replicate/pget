package download

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReaderPromiseParallel(t *testing.T) {
	p := newReaderPromise()
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Deliver([]byte("foobar"), nil)
	}()
	buf, err := io.ReadAll(p)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", string(buf))
	wg.Wait()
}

func TestReaderPromiseReadsWholeChunk(t *testing.T) {
	chunkSize := int64(1024 * 1024)
	p := newReaderPromise()
	data := bytes.Repeat([]byte("x"), int(chunkSize))
	go p.Deliver(data, nil)
	buf := make([]byte, chunkSize)
	// We should only require a single Read() call because all the data should
	// be buffered
	n, err := p.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, data, buf)
	assert.Equal(t, int(chunkSize), n)
}

func TestReaderPromiseDeliverErrPassesErrorsToConsumer(t *testing.T) {
	p := newReaderPromise()

	expectedErr := fmt.Errorf("oh no")

	go p.Deliver(nil, expectedErr)
	buf := make([]byte, 10)
	n, err := p.Read(buf)
	assert.ErrorIs(t, expectedErr, err)
	assert.Equal(t, 0, n)
}

func TestReaderPromiseSubsequentReadsReturnEOF(t *testing.T) {
	p := newReaderPromise()
	go p.Deliver([]byte("foobar"), nil)
	buf, err := io.ReadAll(p)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", string(buf))

	n, err := p.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, io.EOF)
}

package multireader_test

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/replicate/pget/pkg/multireader"
)

func TestBufferedReader_Read(t *testing.T) {
	var wg sync.WaitGroup
	reader := multireader.NewBufferedReader(10)
	_, _ = reader.ReadFrom(bytes.NewReader([]byte("hello world!")))
	p := make([]byte, 5)
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.Equal(t, 12, reader.Len())
		n, err := reader.Read(p)
		assert.Equal(t, 5, n)
		assert.Equal(t, "hello", string(p))
		assert.NoError(t, err)
	}()
	reader.Done()
	wg.Wait()
	assert.Equal(t, 7, reader.Len())
	n, err := reader.Read(p)
	assert.Equal(t, 5, n)
	assert.Equal(t, " worl", string(p))
	assert.Equal(t, 2, reader.Len())
	assert.NoError(t, err)
	n, err = reader.Read(p)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "d!", string(p[:n]))
	assert.Equal(t, 0, reader.Len())
	n, err = reader.Read(p)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestBufferedReader_ReadAt(t *testing.T) {
	var wg sync.WaitGroup
	var content = "The quick brown fox jumps over the lazy dog."
	reader := multireader.NewBufferedReader(10)
	_, _ = reader.ReadFrom(bytes.NewReader([]byte(content)))
	p := make([]byte, 5)
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := reader.ReadAt(p, 5)
		assert.Equal(t, 5, n)
		assert.Equal(t, "uick ", string(p))
		assert.NoError(t, err)
	}()
	reader.Done()
	wg.Wait()
	n, err := reader.ReadAt(p, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, "The q", string(p[:n]))
	assert.NoError(t, err)
	// Check that the reader has not advanced, peek returns starting from the current reader offset
	peeked, err := reader.Peek(int64(len(content)))
	assert.NoError(t, err)
	assert.Equal(t, content, string(peeked))
	// Len should remain the complete content size
	assert.Equal(t, len(content), reader.Len())

	// Check readAt where offset is greater than the content size
	n, err = reader.ReadAt(p, int64(len(content)+1))
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestBufferedReader_ReadFrom(t *testing.T) {
	reader := multireader.NewBufferedReader(10)
	content := []byte("The quick brown fox jumps over the lazy dog.")
	n, err := reader.ReadFrom(bytes.NewReader(content))
	assert.Equal(t, int64(len(content)), n)
	assert.NoError(t, err)
	reader.Done()
	assert.Equal(t, len(content), reader.Len())
	peeked, err := reader.Peek(int64(len(content)))
	assert.NoError(t, err)
	assert.Equal(t, content, peeked)
	// Check behavior after marked ready
	assert.True(t, reader.Ready())
	n, err = reader.ReadFrom(bytes.NewReader(content))
	assert.Equal(t, int64(0), n)
	assert.Equal(t, multireader.ErrReaderMarkedReady, err)
}

func TestBufferedReader_Peek(t *testing.T) {
	content := []byte("The quick brown fox jumps over the lazy dog.")

	tc := []struct {
		name          string
		peekSize      int
		expectedError error
	}{
		{
			name:          "peek size exceeds content size",
			peekSize:      len(content) + 1,
			expectedError: multireader.ErrExceedsCapacity,
		},
		{
			name:          "peek size is less than content size",
			peekSize:      len(content) - 1,
			expectedError: nil,
		},
		{
			name:          "peek size is equal to length of content",
			peekSize:      len(content),
			expectedError: nil,
		},
		{
			name:          "peek size is 0",
			peekSize:      0,
			expectedError: nil,
		},
		{
			name:          "peek size is negative",
			peekSize:      -1,
			expectedError: multireader.ErrNegativeCount,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {

			reader := multireader.NewBufferedReader(int64(len(content)))
			n, err := reader.ReadFrom(bytes.NewReader(content))
			reader.Done()
			require.NoError(t, err)
			assert.Equal(t, int64(len(content)), n)
			peeked, err := reader.Peek(int64(tt.peekSize))
			assert.Equal(t, tt.expectedError, err)
			if err == nil || (tt.peekSize >= len(content) && err == io.EOF) {
				assert.Equal(t, tt.peekSize, len(peeked))
				// Ensure that peek has not advanced the reader
				p := make([]byte, tt.peekSize)
				n, err := reader.Read(p)
				require.NoError(t, err)
				assert.Equal(t, len(content)-tt.peekSize, reader.Len())
				assert.Equal(t, len(p), n)
			}
		})
	}

}

func TestBufferedReader_Done(t *testing.T) {
	var wg sync.WaitGroup
	var done bool
	reader := multireader.NewBufferedReader(10)
	_, _ = reader.ReadFrom(bytes.NewReader([]byte("hello world!")))
	wg.Add(1)
	go func() {
		defer wg.Done()
		done = reader.Ready()
	}()
	assert.False(t, done)
	reader.Done()
	wg.Wait()
	assert.True(t, done)
	assert.True(t, reader.Ready())
	// Ensure that done is idempotent
	reader.Done()
}

func TestBufferedReader_Reset(t *testing.T) {
	reader := multireader.NewBufferedReader(10)
	_, _ = reader.ReadFrom(bytes.NewReader([]byte("hello world!")))
	reader.Reset()
	assert.False(t, reader.Ready())
	reader.Done()
	assert.Equal(t, 0, reader.Len())
}

func TestBufferedReader_Ready(t *testing.T) {
	reader := multireader.NewBufferedReader(10)
	_, _ = reader.ReadFrom(bytes.NewReader([]byte("hello world!")))
	assert.False(t, reader.Ready())
	reader.Done()
	assert.True(t, reader.Ready())

}

func TestBufferedReader_ReadyWait(t *testing.T) {
	var wg sync.WaitGroup
	var ready bool
	reader := multireader.NewBufferedReader(10)
	_, _ = reader.ReadFrom(bytes.NewReader([]byte("hello world!")))
	wg.Add(1)
	go func() {
		defer wg.Done()
		reader.ReadyWait()
		ready = reader.Ready()
	}()
	reader.Done()
	wg.Wait()
	assert.True(t, ready)
}

func TestBufferedReader_Size(t *testing.T) {
	var wg sync.WaitGroup
	var size int
	reader := multireader.NewBufferedReader(10)
	wg.Add(1)
	go func() {
		defer wg.Done()
		size = reader.Len()

	}()
	assert.Equal(t, size, 0)
	err := reader.SetSize(5)
	require.NoError(t, err)
	wg.Wait()
	assert.Equal(t, size, 5)
	_, _ = reader.ReadFrom(bytes.NewReader([]byte("hello world!")))
	reader.Done()
	assert.Equal(t, reader.Len(), 12)
}

func TestBufferedReader_SetSize(t *testing.T) {
	tc := []struct {
		name     string
		capacity int64
		size     int64
	}{
		{
			name:     "size exceeds initial capacity",
			capacity: 10,
			size:     11,
		},
		{
			name:     "size is less than capacity",
			capacity: 10,
			size:     9,
		},
		{
			name:     "size is equal to capacity",
			capacity: 10,
			size:     10,
		},
		{
			name:     "size is 0",
			capacity: 10,
			size:     0,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			var size int
			var wg sync.WaitGroup

			wg.Add(1)

			reader := multireader.NewBufferedReader(tt.capacity)

			go func() {
				defer wg.Done()
				size = reader.Len()
			}()
			assert.Equal(t, 0, size)
			err := reader.SetSize(tt.size)
			require.NoError(t, err)
			if err == nil {
				wg.Wait()
				assert.Equal(t, tt.size, int64(size))
			}
			// Ensure we cannot set the size again
			err = reader.SetSize(5)
			if assert.Error(t, err) {
				assert.Equal(t, err, multireader.ErrSizeAlreadySet)
			}
			// ensure we can call Done after setting the size
			reader.Done()
		})
	}

}

func TestBufferedReader_Len(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	reader := multireader.NewBufferedReader(10)
	_, _ = reader.ReadFrom(bytes.NewReader([]byte("hello world!")))
	go func() {
		defer wg.Done()
		assert.Equal(t, 200, reader.Len())
	}()
	// Test SetSize provides expected length
	_ = reader.SetSize(200)
	wg.Wait()
	reader.Done()
	// Test that done results in the real length
	assert.Equal(t, 12, reader.Len())
	// test that done results in len unblocking as well
	wg.Add(1)
	reader = multireader.NewBufferedReader(10)
	_, _ = reader.ReadFrom(bytes.NewReader([]byte("hello world!")))
	go func() {
		defer wg.Done()
		assert.Equal(t, 12, reader.Len())
	}()
	reader.Done()
	wg.Wait()
}

func TestBufferedReaderEmpty_Read(t *testing.T) {
	reader := multireader.NewBufferedReader(10)
	reader.Done()
	zeroLen := make([]byte, 0)
	n, err := reader.Read(zeroLen)
	assert.Equal(t, 0, n)
	assert.NoError(t, err)
	p := make([]byte, 5)
	n, err = reader.Read(p)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestBufferedReaderEmpty_Peek(t *testing.T) {
	reader := multireader.NewBufferedReader(10)
	reader.Done()
	data, err := reader.Peek(5)
	assert.Equal(t, 0, len(data))
	assert.Equal(t, io.EOF, err)
}

func TestBufferedReaderEmpty_ReadByte(t *testing.T) {
	reader := multireader.NewBufferedReader(10)
	reader.Done()
	_, err := reader.ReadByte()
	assert.Equal(t, io.EOF, err)
}

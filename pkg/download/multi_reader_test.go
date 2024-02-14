package download

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMultiReader(t *testing.T) {
	tests := []struct {
		name      string
		input     io.Reader
		wantErr   bool
		errorText string
	}{
		{
			name:      "ErrorWhenReaderIsNotAChanMultiReader",
			input:     bytes.NewBuffer([]byte("not a chanMultiReader")),
			wantErr:   true,
			errorText: "reader is not a chanMultiReader",
		},
		{
			name: "ErrorWhenChanMultiReaderContainsNonBufferedReader",
			input: func() io.Reader {
				ch := make(chan io.Reader, 1)
				ch <- bytes.NewBuffer([]byte("not a bufferedReader"))
				// explicitly close the channel so that the multiReader can know it's complete
				close(ch)
				return &chanMultiReader{ch: ch}
			}(),
			wantErr:   true,
			errorText: "reader is not a bufferedReader",
		},
		{
			name: "SuccessfullyCreateMultiReader",
			input: func() io.Reader {
				ch := make(chan io.Reader, 1)
				ch <- &bufferedReader{buf: bytes.NewBuffer([]byte("data"))}
				// explicitly close the channel so that the multiReader can know it's complete
				close(ch)
				return &chanMultiReader{ch: ch}
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewMultiReader(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMultiReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && err.Error() != tt.errorText {
				t.Errorf("NewMultiReader() error = %v, wantErr %v", err, tt.errorText)
			}
		})
	}
}

func TestMultiReader_ReadAt(t *testing.T) {
	// Create buffered channel for the multiChanReader so the channel can be closed for the testing case
	count := 10
	expected := ""
	ch := make(chan io.Reader, count)
	for i := 0; i < count; i++ {
		str := strings.Repeat(strconv.Itoa(i), 100)
		expected = expected + str
		br := &bufferedReader{
			buf:     bytes.NewBuffer([]byte(str)),
			size:    int64(len(str)),
			ready:   make(chan struct{}),
			started: make(chan struct{}),
		}
		br.done()
		br.contentLengthReceived()
		ch <- br
	}

	// explicitly close the channel so that the multiReader can know it's complete
	close(ch)
	multiChanReader := &chanMultiReader{ch: ch}
	multiReader, err := NewMultiReader(multiChanReader)
	require.NoError(t, err)

	tests := []struct {
		name         string
		offset       int64
		buffer       []byte
		expectedN    int
		expectedErr  error
		expectedData []byte
	}{
		{
			name:         "Read Within First Reader",
			offset:       0,
			buffer:       make([]byte, 50),
			expectedN:    50,
			expectedErr:  nil,
			expectedData: []byte(expected[:50]),
		},
		{
			name:         "Read Within Last Reader",
			offset:       int64(len(expected) - 75),
			buffer:       make([]byte, 50),
			expectedN:    50,
			expectedErr:  nil,
			expectedData: []byte(expected[len(expected)-75 : len(expected)-25]),
		},
		{
			name:         "Read Across Multiple Readers",
			offset:       50,
			buffer:       make([]byte, 100),
			expectedN:    50,
			expectedErr:  nil,
			expectedData: []byte(expected[50:100]),
		},
		{
			name: "Read Past End Of Last Reader",
			// offset is greater than the total size of the readers
			offset:       int64(len(expected) - 1),
			buffer:       make([]byte, 100),
			expectedN:    1,
			expectedErr:  io.EOF,
			expectedData: []byte(expected[len(expected)-1:]),
		},
		{
			name:        "Read At Negative Offset",
			offset:      -1,
			expectedErr: ErrInvalidOffset,
		},
		{
			name:         "Read At Offset Greater Than Total Size",
			offset:       int64(len(expected)) + 1,
			buffer:       make([]byte, 100),
			expectedN:    0,
			expectedErr:  io.EOF,
			expectedData: []byte{},
		},
		{
			name:         "Read At Offset Equal To Total Size",
			offset:       int64(len(expected)),
			buffer:       make([]byte, 100),
			expectedN:    0,
			expectedErr:  io.EOF,
			expectedData: []byte{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := multiReader.ReadAt(tt.buffer, tt.offset)
			assert.Equal(t, tt.expectedN, n)
			if tt.expectedErr != nil {
				assert.ErrorIs(t, err, tt.expectedErr)
			}
			assert.Equal(t, tt.expectedData, tt.buffer[:tt.expectedN])
			if len(tt.buffer) > tt.expectedN {
				emptyData := bytes.Repeat([]byte{0}, len(tt.buffer)-tt.expectedN)
				assert.Equal(t, tt.buffer[tt.expectedN:], emptyData)
			}
		})
	}

}

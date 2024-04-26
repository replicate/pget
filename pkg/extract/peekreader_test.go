package extract

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPeekReader_Read(t *testing.T) {
	tests := []struct {
		name           string
		readerContents string
		wantBytesPeek  int
		wantBytesRead  int
		wantErr        bool
	}{
		{
			name:           "read from buffer only",
			readerContents: "abc123",
			wantBytesPeek:  6,
			wantBytesRead:  6,
			wantErr:        false,
		},
		{
			name:           "read from reader only",
			readerContents: "abc123",
			wantBytesRead:  3,
			wantErr:        false,
		},
		{
			name:           "read from both buffer and reader",
			readerContents: "abc123",
			wantBytesPeek:  3,
			wantBytesRead:  6,
			wantErr:        false,
		},
		{
			name:           "read empty reader and buffer",
			readerContents: "",
			wantBytesRead:  0,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.readerContents)
			p := &peekReader{reader: reader}
			if tt.wantBytesPeek > 0 {
				peekBytes, err := p.Peek(tt.wantBytesPeek)
				assert.NoError(t, err)
				assert.Equal(t, tt.readerContents[0:tt.wantBytesPeek], string(peekBytes))
			}

			var totalBytesRead int
			var err error
			readBytes := make([]byte, tt.wantBytesRead)
			for totalBytesRead < tt.wantBytesRead && err == nil {
				bytesRead, err := p.Read(readBytes[totalBytesRead:])
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				totalBytesRead += bytesRead
			}
			assert.Equal(t, tt.wantBytesRead, totalBytesRead)
			assert.Equal(t, tt.readerContents[0:tt.wantBytesRead], string(readBytes))
		})
	}
}

package extract

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		name       string
		input      []byte
		expectType string
	}{
		{
			name:       "GZIP",
			input:      []byte{0x1f, 0x8b},
			expectType: "extract.gzipDecompressor",
		},
		{
			name:       "BZIP2",
			input:      []byte{0x42, 0x5a},
			expectType: "extract.bzip2Decompressor",
		},
		{
			name:       "XZ",
			input:      []byte{0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00},
			expectType: "extract.xzDecompressor",
		},
		{
			name:       "Less than 2 bytes",
			input:      []byte{0x1f},
			expectType: "",
		},
		{
			name:       "UNKNOWN",
			input:      []byte{0xde, 0xad},
			expectType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectFormat(tt.input)
			assert.Equal(t, tt.expectType, stringFromInterface(result))
		})
	}
}

func stringFromInterface(i interface{}) string {
	if i == nil {
		return ""
	}
	return fmt.Sprintf("%T", i)
}

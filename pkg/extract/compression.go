package extract

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"compress/lzw"
	"io"

	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"

	"github.com/replicate/pget/pkg/logging"
)

const (
	peekSize = 8
)

var (
	gzipMagic = []byte{0x1F, 0x8B}
	bzipMagic = []byte{0x42, 0x5A}
	xzMagic   = []byte{0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00}
	lzwMagic  = []byte{0x1F, 0x9D}
	lz4Magic  = []byte{0x18, 0x4D, 0x22, 0x04}
)

var _ decompressor = gzipDecompressor{}
var _ decompressor = bzip2Decompressor{}
var _ decompressor = xzDecompressor{}
var _ decompressor = lzwDecompressor{}
var _ decompressor = lz4Decompressor{}

// decompressor represents different compression formats.
type decompressor interface {
	decompress(r io.Reader) (io.Reader, error)
}

// detectFormat returns the appropriate extractor according to the magic number.
func detectFormat(input []byte) decompressor {
	log := logging.GetLogger()
	inputSize := len(input)

	if inputSize < 2 {
		return nil
	}
	// pad to 8 bytes
	if inputSize < 8 {
		input = append(input, make([]byte, peekSize-inputSize)...)
	}

	switch true {
	case bytes.HasPrefix(input, gzipMagic):
		log.Debug().
			Str("type", "gzip").
			Msg("Compression Format")
		return gzipDecompressor{}
	case bytes.HasPrefix(input, bzipMagic):
		log.Debug().
			Str("type", "bzip2").
			Msg("Compression Format")
		return bzip2Decompressor{}
	case bytes.HasPrefix(input, lzwMagic):
		compressionByte := input[2]
		// litWidth is guaranteed to be at least 9 per specification, the high order 3 bits of byte[2] are the litWidth
		// the low order 5 bits are only used by non-unix implementations, we are going to ignore them.
		litWidth := int(compressionByte>>5) + 9
		log.Debug().
			Str("type", "lzw").
			Int("litWidth", litWidth).
			Msg("Compression Format")
		return lzwDecompressor{
			order:    lzw.MSB,
			litWidth: litWidth,
		}
	case bytes.HasPrefix(input, lz4Magic):
		log.Debug().
			Str("type", "lz4").
			Msg("Compression Format")
		return lz4Decompressor{}
	case bytes.HasPrefix(input, xzMagic):
		log.Debug().
			Str("type", "xz").
			Msg("Compression Format")
		return xzDecompressor{}
	default:
		log.Debug().
			Str("type", "none").
			Msg("Compression Format")
		return nil
	}

}

type gzipDecompressor struct{}

func (d gzipDecompressor) decompress(r io.Reader) (io.Reader, error) {
	return gzip.NewReader(r)
}

type bzip2Decompressor struct{}

func (d bzip2Decompressor) decompress(r io.Reader) (io.Reader, error) {
	return bzip2.NewReader(r), nil
}

type xzDecompressor struct{}

func (d xzDecompressor) decompress(r io.Reader) (io.Reader, error) {
	return xz.NewReader(r)
}

type lzwDecompressor struct {
	litWidth int
	order    lzw.Order
}

func (d lzwDecompressor) decompress(r io.Reader) (io.Reader, error) {
	return lzw.NewReader(r, d.order, d.litWidth), nil
}

type lz4Decompressor struct{}

func (d lz4Decompressor) decompress(r io.Reader) (io.Reader, error) {
	return lz4.NewReader(r), nil
}

type noOpDecompressor struct{}

package extract

import (
	"compress/bzip2"
	"compress/gzip"
	"compress/lzw"
	"encoding/binary"
	"io"

	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"

	"github.com/replicate/pget/pkg/logging"
)

const (
	peekSize = 8

	gzipMagic = 0x1F8B
	bzipMagic = 0x425A
	xzMagic   = 0xFD377A585A00
	lzwMagic  = 0x1F9D
	lz4Magic  = 0x184D2204
)

var _ decompressor = gzipDecompressor{}
var _ decompressor = bzip2Decompressor{}
var _ decompressor = xzDecompressor{}
var _ decompressor = lzwDecompressor{}
var _ decompressor = lz4Decompressor{}
var _ decompressor = noOpDecompressor{}

// decompressor represents different compression formats.
type decompressor interface {
	decompress(r io.Reader) (io.Reader, error)
}

// detectFormat returns the appropriate extractor according to the magic number.
func detectFormat(input []byte) decompressor {
	log := logging.GetLogger()
	inputSize := len(input)

	if inputSize < 2 {
		return noOpDecompressor{}
	}
	// pad to 8 bytes
	if inputSize < 8 {
		input = append(input, make([]byte, peekSize-inputSize)...)
	}

	magic16 := binary.BigEndian.Uint16(input)
	magic32 := binary.BigEndian.Uint32(input)
	// We need to pre-pend the padding since we're reading into something bigendian and exceeding the
	// 48bits size of the magic number bytes. The 16 and 32 bit magic numbers are complete bytes and
	// therefore do not need any padding.
	magic48 := binary.BigEndian.Uint64(append(make([]byte, 2), input[0:6]...))

	switch true {
	case magic16 == gzipMagic:
		log.Debug().
			Str("type", "gzip").
			Msg("Compression Format")
		return gzipDecompressor{}
	case magic16 == bzipMagic:
		log.Debug().
			Str("type", "bzip2").
			Msg("Compression Format")
		return bzip2Decompressor{}
	case magic16 == lzwMagic:
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
	case magic32 == lz4Magic:
		log.Debug().
			Str("type", "lz4").
			Msg("Compression Format")
		return lz4Decompressor{}
	case magic48 == xzMagic:
		log.Debug().
			Str("type", "xz").
			Msg("Compression Format")
		return xzDecompressor{}
	default:
		log.Debug().
			Str("type", "none").
			Msg("Compression Format")
		return noOpDecompressor{}
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

func (d noOpDecompressor) decompress(r io.Reader) (io.Reader, error) {
	return r, nil
}

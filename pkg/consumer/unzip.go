package consumer

import (
	"fmt"
	"io"

	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/extract"
)

type ZipExtractor struct {
	overwrite bool
}

var _ Consumer = &ZipExtractor{}

func (f *ZipExtractor) Consume(reader io.Reader, destPath string, size int64) error {
	readerAt, err := download.NewMultiReader(reader)
	if err != nil {
		return fmt.Errorf("error converting to multi reader: %w", err)
	}
	err = extract.ZipFile(readerAt, destPath, size, f.overwrite)
	if err != nil {
		return fmt.Errorf("error extracting file: %w", err)
	}
	return nil
}

func (f *ZipExtractor) EnableOverwrite() {
	f.overwrite = true
}

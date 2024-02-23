package consumer

import (
	"fmt"
	"io"

	"github.com/replicate/pget/pkg/extract"
)

type TarExtractor struct {
	overwrite bool
}

var _ Consumer = &TarExtractor{}

func (f *TarExtractor) Consume(reader io.Reader, destPath string, fileSize int64) error {
	err := extract.TarFile(reader, destPath, f.overwrite)
	if err != nil {
		return fmt.Errorf("error extracting file: %w", err)
	}
	return nil
}

func (f *TarExtractor) EnableOverwrite() {
	f.overwrite = true
}

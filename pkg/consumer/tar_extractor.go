package consumer

import (
	"fmt"
	"io"

	"github.com/replicate/pget/pkg/extract"
)

type TarExtractor struct{}

var _ Consumer = &TarExtractor{}

func (f *TarExtractor) Consume(reader io.Reader, destPath string, _ int64) error {
	err := extract.TarFile(reader, destPath)
	if err != nil {
		return fmt.Errorf("error extracting file: %w", err)
	}
	return nil
}

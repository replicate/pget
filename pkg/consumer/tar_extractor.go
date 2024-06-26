package consumer

import (
	"bufio"
	"fmt"
	"io"

	"github.com/replicate/pget/pkg/extract"
)

type TarExtractor struct {
	Overwrite bool
}

var _ Consumer = &TarExtractor{}

func (f *TarExtractor) Consume(reader io.Reader, destPath string) error {
	err := extract.TarFile(bufio.NewReader(reader), destPath, f.Overwrite)
	if err != nil {
		return fmt.Errorf("error extracting file: %w", err)
	}
	return nil
}

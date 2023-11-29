package consumer

import (
	"fmt"
	"io"

	"github.com/replicate/pget/pkg/extract"
)

type TarExtracter struct{}

var _ Consumer = &TarExtracter{}

func (f *TarExtracter) Consume(reader io.Reader, destPath string) error {
	err := extract.ExtractTarFile(reader, destPath)
	if err != nil {
		return fmt.Errorf("error extracting file: %w", err)
	}
	return nil
}

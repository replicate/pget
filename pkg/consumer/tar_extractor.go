package consumer

import (
	"fmt"
	"io"

	"github.com/replicate/pget/pkg/extract"
)

type TarExtractor struct {
	force bool
}

var _ Consumer = &TarExtractor{}

func (f *TarExtractor) Consume(reader io.Reader, destPath string) error {
	err := extract.TarFile(reader, destPath, f.force)
	if err != nil {
		return fmt.Errorf("error extracting file: %w", err)
	}
	return nil
}

func (f *TarExtractor) SetOverwriteTarget(force bool) {
	f.force = force
}

package consumer

import (
	"fmt"
	"io"
	"os"
)

type FileWriter struct{}

var _ Consumer = &FileWriter{}

func (f *FileWriter) Consume(reader io.Reader, destPath string) error {
	out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	defer out.Close()

	_, err = io.Copy(out, reader)
	if err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	return nil
}

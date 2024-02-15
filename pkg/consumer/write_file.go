package consumer

import (
	"fmt"
	"io"
	"os"
)

type FileWriter struct {
	overwrite bool
}

var _ Consumer = &FileWriter{}

func (f *FileWriter) Consume(reader io.Reader, destPath string, fileSize int64, contentType string) error {
	openFlags := os.O_WRONLY | os.O_CREATE
	if f.overwrite {
		openFlags |= os.O_TRUNC
	}
	out, err := os.OpenFile(destPath, openFlags, 0644)
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

func (f *FileWriter) EnableOverwrite() {
	f.overwrite = true
}

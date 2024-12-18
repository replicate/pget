package consumer

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FileWriter struct {
	Overwrite bool
}

var _ Consumer = &FileWriter{}

func (f *FileWriter) Consume(reader io.Reader, destPath string, expectedBytes int64) error {
	openFlags := os.O_WRONLY | os.O_CREATE
	targetDir := filepath.Dir(destPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("error creating directory: %w", err)
	}
	if f.Overwrite {
		openFlags |= os.O_TRUNC
	}
	out, err := os.OpenFile(destPath, openFlags, 0644)
	if err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	defer out.Close()

	written, err := io.Copy(out, reader)
	if err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}

	if written != expectedBytes {
		return fmt.Errorf("expected %d bytes, wrote %d", expectedBytes, written)
	}
	return nil
}

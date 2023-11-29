package consumer

import (
	"fmt"
	"io"
	"os"
)

type FileWriter struct{}

var _ Consumer = &FileWriter{}

func (f *FileWriter) Consume(reader io.Reader, destPath string) error {
	// NOTE(morgan): We check if the file exists early on allowing a fast fail, it is safe
	// to just apply os.O_TRUNC. Getting to this point without checking existence and
	// the `--force` flag is a programming error further up the stack.
	out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
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

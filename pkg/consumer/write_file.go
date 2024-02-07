package consumer

import (
	"fmt"
	"io"
	"os"
)

type FileWriter struct {
	force bool
}

var _ Consumer = &FileWriter{}

func (f *FileWriter) Consume(reader io.Reader, destPath string) error {
	// NOTE(morgan): We check if the file exists early on allowing a fast fail, it is safe
	// to just apply os.O_TRUNC. Getting to this point without checking existence and
	// the `--force` flag is a programming error further up the stack.
	flags := os.O_WRONLY | os.O_CREATE
	if f.force {
		flags |= os.O_TRUNC
	}
	out, err := os.OpenFile(destPath, flags, 0644)
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

func (f *FileWriter) SetOverwriteTarget(force bool) {
	f.force = force
}

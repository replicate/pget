package consumer

import (
	"fmt"
	"io"
	"os"
)

var _ Consumer = &StdoutConsumer{}

type StdoutConsumer struct {
}

func (s StdoutConsumer) Consume(reader io.Reader, destPath string, fileSize int64) error {
	_, err := io.Copy(os.Stdout, reader)
	if err != nil {
		return fmt.Errorf("error writing to stdout: %w", err)
	}
	return nil
}

func (s StdoutConsumer) EnableOverwrite() {
	// no op
}

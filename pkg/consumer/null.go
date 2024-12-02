package consumer

import (
	"fmt"
	"io"
)

type NullWriter struct{}

var _ Consumer = &NullWriter{}

func (NullWriter) Consume(reader io.Reader, destPath string, expectedBytes int64) error {
	// io.Discard is explicitly designed to always succeed, ignore errors.
	bytesRead, _ := io.Copy(io.Discard, reader)
	if bytesRead != expectedBytes {
		return fmt.Errorf("expected %d bytes, read %d", expectedBytes, bytesRead)
	}
	return nil
}

package consumer

import (
	"io"
)

type NullWriter struct{}

var _ Consumer = &NullWriter{}

func (f *NullWriter) Consume(reader io.Reader, destPath string) error {
	// io.Discard is explicitly designed to always succeed, ignore errors.
	_, _ = io.Copy(io.Discard, reader)
	return nil
}

func (f *NullWriter) EnableOverwrite() {
	// no-op
}

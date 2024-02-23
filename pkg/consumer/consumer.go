package consumer

import "io"

type Consumer interface {
	Consume(reader io.Reader, destPath string, fileSize int64) error
	EnableOverwrite()
}

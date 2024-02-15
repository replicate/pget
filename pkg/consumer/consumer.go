package consumer

import "io"

type Consumer interface {
	Consume(reader io.Reader, destPath string, fileSize int64) error
	// EnableOverwrite sets the overwrite flag for the consumer, allowing it to overwrite files if necessary/supported
	EnableOverwrite()
}

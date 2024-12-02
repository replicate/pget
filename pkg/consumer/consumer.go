package consumer

import "io"

type Consumer interface {
	Consume(reader io.Reader, destPath string, expectedBytes int64) error
}

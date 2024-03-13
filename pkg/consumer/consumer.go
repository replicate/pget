package consumer

import "io"

type Consumer interface {
	Consume(reader io.Reader, url string, destPath string) error
}

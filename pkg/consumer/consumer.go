package consumer

import "io"

type Consumer interface {
	Consume(reader io.Reader, destPath string) error
	SetOverwriteTarget(force bool)
}

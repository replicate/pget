//go:build darwin

package consumer

import (
	"io"

	"github.com/replicate/pget/pkg/logging"
)

var _ Consumer = &VMSpliceConsumer{}

type VMSpliceConsumer struct {
}

func (v VMSpliceConsumer) Consume(reader io.Reader, destPath string, fileSize int64) error {
	logger := logging.GetLogger()
	logger.Warn().Msg("'vmsplice' is not supported on darwin, falling back to StdoutConsumer")
	return StdoutConsumer{}.Consume(reader, destPath, fileSize)

}

func (v VMSpliceConsumer) EnableOverwrite() {
	// no op
}

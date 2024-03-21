//go:build linux

package consumer

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"

	"github.com/dustin/go-humanize"
)

var _ Consumer = &VMSpliceConsumer{}

type VMSpliceConsumer struct{}

func (v VMSpliceConsumer) Consume(reader io.Reader, destPath string, fileSize int64) error {
	// Create the buffer once and reuse it, this is zero additional allocations
	buffer := make([]byte, humanize.MiByte)
	for {
		length, err := reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("error reading from reader: %w", err)
		}
		if length > 0 {
			_, _, err := syscall.Syscall6(
				syscall.SYS_VMSPLICE,
				os.Stdout.Fd(),
				uintptr(unsafe.Pointer(
					&syscall.Iovec{
						Base: &buffer[0],
						Len:  uint64(length),
					})), 1, 0, 0, 0)
			if err != 0 {
				return fmt.Errorf("error splicing %s to stdout: %w", buffer, err)
			}
		}
	}
}

func (v VMSpliceConsumer) EnableOverwrite() {
	// no op
}

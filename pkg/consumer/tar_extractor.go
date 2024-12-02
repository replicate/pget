package consumer

import (
	"bufio"
	"fmt"
	"io"

	"github.com/replicate/pget/pkg/extract"
)

type TarExtractor struct {
	Overwrite bool
}

var _ Consumer = &TarExtractor{}

var _ io.Reader = &byteTrackingReader{}

type byteTrackingReader struct {
	bytesRead int64
	r         io.Reader
}

func (b *byteTrackingReader) Read(p []byte) (n int, err error) {
	n, err = b.r.Read(p)
	b.bytesRead += int64(n)
	return
}

func (f *TarExtractor) Consume(reader io.Reader, destPath string, expectedBytes int64) error {
	btReader := &byteTrackingReader{r: reader}
	err := extract.TarFile(bufio.NewReader(btReader), destPath, f.Overwrite)
	if err != nil {
		return fmt.Errorf("error extracting file: %w", err)
	}
	if btReader.bytesRead != expectedBytes {
		return fmt.Errorf("expected %d bytes, read %d from archive", expectedBytes, btReader.bytesRead)
	}
	return nil
}

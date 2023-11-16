package download

import (
	"context"
	"fmt"

	"github.com/replicate/pget/pkg/extract"
)

type ExtractTarMode struct {
}

func (m *ExtractTarMode) DownloadFile(url string, dest string) error {
	ctx := context.Background()
	target := Target{URL: url, TrueURL: url, Dest: dest}
	downloader := &BufferMode{}
	buffer, fileSize, err := downloader.fileToBuffer(ctx, target)
	if err != nil {
		return fmt.Errorf("error downloading file: %w", err)
	}
	err = extract.ExtractTarFile(buffer, dest, fileSize)
	if err != nil {
		return fmt.Errorf("error extracting file: %w", err)
	}
	return nil
}

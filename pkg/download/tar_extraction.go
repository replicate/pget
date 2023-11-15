package download

import (
	"fmt"
	"github.com/replicate/pget/pkg/client"

	"github.com/replicate/pget/pkg/extract"
)

type ExtractTarMode struct {
}

func (m *ExtractTarMode) DownloadFile(url string, dest string) error {
	downloader := &BufferMode{Client: client.NewClient()}
	buffer, fileSize, err := downloader.fileToBuffer(url)
	if err != nil {
		return fmt.Errorf("error downloading file: %w", err)
	}
	err = extract.ExtractTarFile(buffer, dest, fileSize)
	if err != nil {
		return fmt.Errorf("error extracting file: %w", err)
	}
	return nil
}

package download

import (
	"context"
	"fmt"
	"time"

	"github.com/replicate/pget/pkg/extract"
)

type ExtractTarMode struct {
}

func (m *ExtractTarMode) DownloadFile(url string, dest string) (int64, time.Duration, error) {
	ctx := context.Background()
	startTime := time.Now()
	target := Target{URL: url, TrueURL: url, Dest: dest}
	downloader := &BufferMode{}
	buffer, fileSize, err := downloader.fileToBuffer(ctx, target)
	if err != nil {
		return int64(-1), 0, fmt.Errorf("error downloading file: %w", err)
	}
	elapsedTime := time.Since(startTime)
	err = extract.ExtractTarFile(buffer, dest, fileSize)
	if err != nil {
		return int64(-1), 0, fmt.Errorf("error extracting file: %w", err)
	}
	return fileSize, elapsedTime, nil
}

package download

import (
	"context"
	"fmt"
	"github.com/replicate/pget/pkg/client"
	"time"

	"github.com/replicate/pget/pkg/extract"
)

const ExtractTarModeName = "tar-extract"

type ExtractTarMode struct {
	BufferMode
}

func getExtractTarMode(config ModeConfiguration) Mode {
	return &ExtractTarMode{
		BufferMode: BufferMode{
			Client: client.NewHTTPClient(config.forceHTTP2, config.maxConnPerHost),
		},
	}
}

func (m *ExtractTarMode) DownloadFile(url string, dest string) (int64, time.Duration, error) {
	ctx := context.Background()
	startTime := time.Now()
	target := Target{URL: url, TrueURL: url, Dest: dest}
	buffer, fileSize, err := m.fileToBuffer(ctx, target)
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

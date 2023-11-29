package pget

import (
	"context"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/replicate/pget/pkg/consumer"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/logging"
)

type Getter struct {
	Downloader download.Strategy
	Consumer   consumer.Consumer
}

func (g *Getter) DownloadFile(ctx context.Context, url string, dest string) (int64, time.Duration, error) {
	if g.Consumer == nil {
		g.Consumer = &consumer.FileWriter{}
	}
	logger := logging.GetLogger()
	downloadStartTime := time.Now()
	buffer, fileSize, err := g.Downloader.Fetch(ctx, url)
	if err != nil {
		return fileSize, 0, err
	}
	downloadElapsed := time.Since(downloadStartTime)
	writeStartTime := time.Now()

	err = g.Consumer.Consume(buffer, dest)
	if err != nil {
		return fileSize, 0, fmt.Errorf("error writing file: %w", err)
	}
	writeElapsed := time.Since(writeStartTime)
	totalElapsed := time.Since(downloadStartTime)

	size := humanize.Bytes(uint64(fileSize))
	downloadThroughput := humanize.Bytes(uint64(float64(fileSize) / downloadElapsed.Seconds()))
	writeThroughput := humanize.Bytes(uint64(float64(fileSize) / writeElapsed.Seconds()))
	logger.Info().
		Str("dest", dest).
		Str("size", size).
		Str("download_throughput", fmt.Sprintf("%s/s", downloadThroughput)).
		Str("download_elapsed", fmt.Sprintf("%.3fs", downloadElapsed.Seconds())).
		Str("write_throughput", fmt.Sprintf("%s/s", writeThroughput)).
		Str("write_elapsed", fmt.Sprintf("%.3fs", writeElapsed.Seconds())).
		Str("total_elapsed", fmt.Sprintf("%.3fs", totalElapsed.Seconds())).
		Msg("Complete")
	return fileSize, totalElapsed, nil

}

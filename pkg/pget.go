package pget

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/consumer"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/logging"
)

type Getter struct {
	Downloader download.Strategy
	Consumer   consumer.Consumer
	Options    Options
}

type Options struct {
	MaxConcurrentFiles int
}

type ManifestEntry struct {
	URL  string
	Dest string
}

// A Manifest is a slice of ManifestEntry, with a helper method to add entries
type Manifest []ManifestEntry

func (m Manifest) AddEntry(url string, destination string) Manifest {
	return append(m, ManifestEntry{URL: url, Dest: destination})
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
	// downloadElapsed := time.Since(downloadStartTime)
	// writeStartTime := time.Now()

	err = g.Consumer.Consume(buffer, dest, fileSize)
	if err != nil {
		return fileSize, 0, fmt.Errorf("error writing file: %w", err)
	}
	// writeElapsed := time.Since(writeStartTime)
	totalElapsed := time.Since(downloadStartTime)

	size := humanize.Bytes(uint64(fileSize))
	// downloadThroughput := humanize.Bytes(uint64(float64(fileSize) / downloadElapsed.Seconds()))
	// writeThroughput := humanize.Bytes(uint64(float64(fileSize) / writeElapsed.Seconds()))
	logger.Info().
		Str("dest", dest).
		Str("url", url).
		Str("size", size).
		// Str("download_throughput", fmt.Sprintf("%s/s", downloadThroughput)).
		// Str("download_elapsed", fmt.Sprintf("%.3fs", downloadElapsed.Seconds())).
		// Str("write_throughput", fmt.Sprintf("%s/s", writeThroughput)).
		// Str("write_elapsed", fmt.Sprintf("%.3fs", writeElapsed.Seconds())).
		Str("total_elapsed", fmt.Sprintf("%.3fs", totalElapsed.Seconds())).
		Msg("Complete")
	return fileSize, totalElapsed, nil
}

func (g *Getter) DownloadFiles(ctx context.Context, manifest Manifest) (int64, time.Duration, error) {
	if g.Consumer == nil {
		g.Consumer = &consumer.FileWriter{}
	}

	errGroup, ctx := errgroup.WithContext(ctx)

	if g.Options.MaxConcurrentFiles != 0 {
		errGroup.SetLimit(g.Options.MaxConcurrentFiles)
	}

	totalSize := new(atomic.Int64)
	multifileDownloadStart := time.Now()

	err := g.downloadFilesFromManifest(ctx, errGroup, manifest, totalSize)
	if err != nil {
		return 0, 0, fmt.Errorf("error initiating download of files from manifest: %w", err)
	}
	err = errGroup.Wait()
	if err != nil {
		return 0, 0, fmt.Errorf("error downloading files: %w", err)
	}
	elapsedTime := time.Since(multifileDownloadStart)
	return totalSize.Load(), elapsedTime, nil
}

func (g *Getter) downloadFilesFromManifest(ctx context.Context, eg *errgroup.Group, entries []ManifestEntry, totalSize *atomic.Int64) error {
	logger := logging.GetLogger()

	for _, entry := range entries {
		// Avoid the `entry` loop variable being captured by the
		// goroutine by creating new variables
		url, dest := entry.URL, entry.Dest
		logger.Debug().Str("url", url).Str("dest", dest).Msg("Queueing Download")

		eg.Go(func() error {
			return g.downloadAndMeasure(ctx, url, dest, totalSize)
		})
	}
	return nil
}

func (g *Getter) downloadAndMeasure(ctx context.Context, url, dest string, totalSize *atomic.Int64) error {
	fileSize, _, err := g.DownloadFile(ctx, url, dest)
	if err != nil {
		return err
	}
	totalSize.Add(fileSize)
	return nil
}

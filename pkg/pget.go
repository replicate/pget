package pget

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/version"

	"github.com/dustin/go-humanize"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/consumer"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/logging"
)

type MetricsPayload struct {
	Source string         `json:"source,omitempty"`
	Type   string         `json:"type,omitempty"`
	Data   map[string]any `json:"data,omitempty"`
}

type Getter struct {
	Downloader download.Strategy
	Consumer   consumer.Consumer
	Options    Options
}

type Options struct {
	MaxConcurrentFiles int
	MetricsEndpoint    string
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
		// Fire and forget metrics
		go func() {
			g.sendMetrics(url, fileSize, 0, err)
		}()
		return fileSize, 0, fmt.Errorf("error writing file: %w", err)
	}

	// writeElapsed := time.Since(writeStartTime)
	totalElapsed := time.Since(downloadStartTime)

	// Fire and forget metrics
	go func() {
		g.sendMetrics(url, fileSize, (float64(fileSize) / totalElapsed.Seconds()), nil)
	}()

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

func (g *Getter) sendMetrics(url string, size int64, throughput float64, err error) {
	logger := logging.GetLogger()
	endpoint := viper.GetString(config.OptMetricsEndpoint)
	if endpoint == "" {
		return
	}

	data := map[string]any{"url": url, "size": size, "version": version.GetVersion()}
	if err != nil {
		data["error"] = err.Error()
	} else {
		data["bytes_per_second"] = throughput
	}

	payload := MetricsPayload{
		Source: "pget",
		Type:   "download",
		Data:   data,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		logger.Debug().Err(err).Any("payload", payload).Msg("Error marshalling metrics")
		return
	}
	// Ignore error and response
	resp, err := http.DefaultClient.Post(endpoint, "application/json", bytes.NewBuffer(body))
	if err != nil {
		logger.Debug().Err(err).Str("endpoint", endpoint).Msg("Error sending metrics")
		return
	}
	if resp.StatusCode != http.StatusOK {
		logger.Debug().Int("status_code", resp.StatusCode).Str("endpoint", endpoint).Msg("Error sending metrics")
	}
}

package download

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/logging"
	"github.com/replicate/pget/pkg/optname"
)

type BufferMode struct {
	Client *http.Client
}

type Target struct {
	URL     string
	TrueURL string
	Dest    string
}

func (t *Target) Basename() string {
	return filepath.Base(t.Dest)
}

func (m *BufferMode) getRemoteFileSize(ctx context.Context, url string) (string, int64, error) {
	// Acquire a client for the head request
	// Acquire a client for a download
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return "", int64(-1), fmt.Errorf("failed create request for %s", req.URL.String())
	}
	httpClient, err := client.AcquireClient(req.URL.Host)
	if err != nil {
		return "", int64(-1), fmt.Errorf("error acquiring client for %s: %w", req.URL.String(), err)
	}
	defer httpClient.Done()

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", int64(-1), err
	}
	defer resp.Body.Close()
	trueUrl := resp.Request.URL.String()
	if trueUrl != url {
		logging.Logger.Info().Str("url", url).Str("redirect_url", trueUrl).Msg("Redirect")
	}

	fileSize := resp.ContentLength
	if fileSize <= 0 {
		return "", int64(-1), fmt.Errorf("unable to determine file size")
	}
	return trueUrl, fileSize, nil
}

func (m *BufferMode) fileToBuffer(ctx context.Context, target Target) (*bytes.Buffer, int64, error) {
	maxConcurrency := viper.GetInt(optname.Concurrency)

	trueURL, fileSize, err := m.getRemoteFileSize(ctx, target.URL)
	if err != nil {
		return nil, -1, err
	}
	if trueURL != target.URL {
		target.TrueURL = trueURL
	}

	minChunkSize, err := humanize.ParseBytes(viper.GetString(optname.MinimumChunkSize))
	if err != nil {
		return nil, -1, fmt.Errorf("unable to parse minimum chunk size: %v", err)
	}
	chunkSize := int64(minChunkSize)
	if chunkSize < 0 {
		return nil, -1, fmt.Errorf("error: chunksize incorrect - result is negative, %d", chunkSize)
	}
	// not more than one connection per min chunk size
	concurrency := int(math.Ceil(float64(fileSize) / float64(chunkSize)))

	if concurrency > maxConcurrency {
		concurrency = maxConcurrency
		chunkSize = fileSize / int64(concurrency)
	}
	logging.Logger.Debug().Str("dest", target.Dest).
		Str("url", target.URL).
		Int64("size", fileSize).
		Int("connections", concurrency).
		Int64("chunkSize", chunkSize).
		Msg("Downloading")

	errGroup, ctx := errgroup.WithContext(ctx)

	data := make([]byte, fileSize)
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1

		if i == concurrency-1 {
			end = fileSize - 1
		}

		errGroup.Go(func() error {
			return m.downloadChunk(ctx, start, end, data[start:end+1], trueURL)
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, -1, err // return the first error we encounter
	}

	elapsed := time.Since(startTime)
	througput := fmt.Sprintf("%s/s", humanize.Bytes(uint64(float64(fileSize)/elapsed.Seconds())))
	logging.Logger.Info().Str("url", target.URL).
		Str("dest", target.Dest).
		Str("size", humanize.Bytes(uint64(fileSize))).
		Str("elapsed", fmt.Sprintf("%.3fs", elapsed.Seconds())).
		Str("throughput", througput).
		Msg("Complete")

	buffer := bytes.NewBuffer(data)
	return buffer, fileSize, nil
}

func (m *BufferMode) downloadChunk(ctx context.Context, start, end int64, dataSlice []byte, trueURL string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", trueURL, nil)
	if err != nil {
		return fmt.Errorf("failed to download %s", req.URL.String())
	}

	// Acquire a client for a download
	httpClient, err := client.AcquireClient(req.URL.Host)
	if err != nil {
		return fmt.Errorf("error acquiring client for %s: %w", req.URL.String(), err)
	}
	defer httpClient.Done()

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request for %s: %w", req.URL.String(), err)
	}
	defer resp.Body.Close()

	n, err := io.ReadFull(resp.Body, dataSlice)
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading response for %s: %w", req.URL.String(), err)
	}
	if n != int(end-start+1) {
		return fmt.Errorf("downloaded %d bytes instead of %d for %s", n, end-start+1, req.URL.String())
	}
	return nil
}

func (m *BufferMode) DownloadFile(url string, dest string) error {
	ctx := context.Background()
	target := Target{URL: url, TrueURL: url, Dest: dest}
	buffer, _, err := m.fileToBuffer(ctx, target)
	if err != nil {
		return err
	}
	err = os.WriteFile(dest, buffer.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	return nil
}

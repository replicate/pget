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
	URL           string
	TrueURL       string
	Dest          string
	SchemeHostKey string
}

func (t *Target) Basename() string {
	return filepath.Base(t.Dest)
}

func (m *BufferMode) getRemoteFileSize(ctx context.Context, target Target) (string, int64, error) {
	// Acquire a client for the head request
	// Acquire a client for a download
	req, err := http.NewRequestWithContext(ctx, "HEAD", target.URL, nil)
	if err != nil {
		return "", int64(-1), fmt.Errorf("failed create request for %s", req.URL.String())
	}
	httpClient, err := client.AcquireClient(target.SchemeHostKey)
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
	if trueUrl != target.URL {
		logging.Logger.Info().Str("url", target.URL).Str("redirect_url", trueUrl).Msg("Redirect")
	}

	fileSize := resp.ContentLength
	if fileSize <= 0 {
		return "", int64(-1), fmt.Errorf("unable to determine file size")
	}
	return trueUrl, fileSize, nil
}

func (m *BufferMode) fileToBuffer(ctx context.Context, target Target) (*bytes.Buffer, int64, error) {
	maxChunks := viper.GetInt(optname.MaxChunks)

	trueURL, fileSize, err := m.getRemoteFileSize(ctx, target)
	if err != nil {
		return nil, -1, err
	}
	if trueURL != target.URL {
		target.TrueURL = trueURL
	}

	// TODO split this into separate functions
	minChunkSize, err := humanize.ParseBytes(viper.GetString(optname.MinimumChunkSize))
	if err != nil {
		return nil, -1, fmt.Errorf("unable to parse minimum chunk size: %v", err)
	}
	chunkSize := fileSize / int64(maxChunks)
	if chunkSize < int64(minChunkSize) {
		chunkSize = int64(minChunkSize)
	}
	if chunkSize < 0 {
		return nil, -1, fmt.Errorf("error: chunksize incorrect - result is negative, %d", chunkSize)
	}
	// not more than one connection per min chunk size
	chunks := int(math.Ceil(float64(fileSize) / float64(chunkSize)))

	if chunks > maxChunks {
		chunks = maxChunks
		chunkSize = fileSize / int64(chunks)
	}
	logging.Logger.Debug().Str("dest", target.Dest).
		Str("url", target.URL).
		Int64("size", fileSize).
		Int("connections", chunks).
		Int64("chunkSize", chunkSize).
		Msg("Downloading")

	errGroup, ctx := errgroup.WithContext(ctx)

	data := make([]byte, fileSize)
	startTime := time.Now()

	for i := 0; i < chunks; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1

		if i == chunks-1 {
			end = fileSize - 1
		}

		errGroup.Go(func() error {
			return m.downloadChunk(ctx, start, end, data[start:end+1], target)
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

func (m *BufferMode) downloadChunk(ctx context.Context, start, end int64, dataSlice []byte, target Target) error {
	req, err := http.NewRequestWithContext(ctx, "GET", target.TrueURL, nil)
	if err != nil {
		return fmt.Errorf("failed to download %s", req.URL.String())
	}

	// Acquire a client for a download
	httpClient, err := client.AcquireClient(target.SchemeHostKey)
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

func (m *BufferMode) DownloadFile(url string, dest string) (int64, time.Duration, error) {
	ctx := context.Background()
	schemeHost, err := client.GetSchemeHostKey(url)
	if err != nil {
		return int64(-1), 0, fmt.Errorf("error getting scheme host key: %w", err)
	}
	downloadStartTime := time.Now()
	target := Target{URL: url, TrueURL: url, Dest: dest, SchemeHostKey: schemeHost}
	buffer, fileSize, err := m.fileToBuffer(ctx, target)
	if err != nil {
		return fileSize, 0, err
	}
	downloadCompleteDuration := time.Since(downloadStartTime)
	writeStartTime := time.Now()
	err = os.WriteFile(dest, buffer.Bytes(), 0644)
	if err != nil {
		return fileSize, downloadCompleteDuration, fmt.Errorf("error writing file: %w", err)
	}
	writeElapsed := time.Since(writeStartTime)
	logging.Logger.Debug().
		Str("dest", dest).
		Str("elapsed", fmt.Sprintf("%.3fs", writeElapsed.Seconds())).
		Msg("Write Complete")
	return fileSize, downloadCompleteDuration, nil
}

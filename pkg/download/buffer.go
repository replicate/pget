package download

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/optname"
)

type BufferMode struct {
	Client *http.Client
}

func (m *BufferMode) getRemoteFileSize(url string) (string, int64, error) {
	resp, err := m.Client.Head(url)
	if err != nil {
		return "", int64(-1), err
	}
	defer resp.Body.Close()
	trueUrl := resp.Request.URL.String()
	if trueUrl != url {
		if viper.GetBool(optname.Verbose) {
			fmt.Printf("Redirected to %s\n", trueUrl)
		}
	}

	fileSize := resp.ContentLength
	if fileSize <= 0 {
		return "", int64(-1), fmt.Errorf("unable to determine file size")
	}
	return trueUrl, fileSize, nil
}

func (m *BufferMode) fileToBuffer(url string) (*bytes.Buffer, int64, error) {
	maxConcurrency := viper.GetInt(optname.Concurrency)

	trueURL, fileSize, err := m.getRemoteFileSize(url)
	if err != nil {
		return nil, -1, err
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
	if viper.GetBool(optname.Verbose) {
		fmt.Printf("Downloading %s bytes with %d connections (chunk-size = %s)\n", humanize.Bytes(uint64(fileSize)), concurrency, humanize.Bytes(uint64(chunkSize)))
	}

	errGroup, ctx := errgroup.WithContext(context.Background())

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

	elapsed := time.Since(startTime).Seconds()
	througput := humanize.Bytes(uint64(float64(fileSize) / elapsed))
	fmt.Printf("Downloaded %s bytes in %.3fs (%s/s)\n", humanize.Bytes(uint64(fileSize)), elapsed, througput)

	buffer := bytes.NewBuffer(data)
	return buffer, fileSize, nil
}

func (m *BufferMode) downloadChunk(ctx context.Context, start, end int64, dataSlice []byte, trueURL string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", trueURL, nil)
	if err != nil {
		return fmt.Errorf("failed to download %s", req.URL.String())
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := m.Client.Do(req)
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
	buffer, _, err := m.fileToBuffer(url)
	if err != nil {
		return err
	}
	err = os.WriteFile(dest, buffer.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	return nil
}

package download

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/config"
)

func getRemoteFileSize(url string) (string, int64, error) {
	// TODO: this needs a retry
	resp, err := http.DefaultClient.Head(url)
	if err != nil {
		return "", int64(-1), err
	}
	defer resp.Body.Close()
	trueUrl := resp.Request.URL.String()
	if trueUrl != url {
		fmt.Printf("Redirected to %s\n", trueUrl)
	}

	fileSize := resp.ContentLength
	if fileSize <= 0 {
		return "", int64(-1), fmt.Errorf("unable to determine file size")
	}
	return trueUrl, fileSize, nil
}

func FileToBuffer(url string) (*bytes.Buffer, int64, error) {
	verboseMode := viper.GetBool("verbose")
	maxConcurrency := viper.GetInt("concurrency")
	trueUrl, fileSize, err := getRemoteFileSize(url)
	if err != nil {
		return nil, -1, err
	}

	minChunkSize, err := humanize.ParseBytes(viper.GetString(config.MinimumChunkSize))
	if err != nil {
		return nil, fileSize, err
	}
	// Get the chunkSize, which has a floor of minChunkSize
	chunkSize := int64(math.Max(float64(minChunkSize), float64(fileSize/int64(maxConcurrency))))

	// set real concurrency, it may be lower than maxConcurrency depending on chunkSize
	// Get the lowest of maxConcurrency and the number of chunks we need to download
	concurrency := int(math.Min(float64(maxConcurrency), math.Ceil(float64(fileSize)/float64(chunkSize))))

	if verboseMode {
		fmt.Printf("Downloading %s bytes with %d connections (chunk-size = %s)\n", humanize.Bytes(uint64(fileSize)), concurrency, humanize.Bytes(uint64(chunkSize)))
	}

	var errGroup errgroup.Group

	data := make([]byte, fileSize)
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1

		if i == concurrency-1 {
			end = fileSize - 1
		}
		errGroup.Go(func() error { return downloadChunk(start, end, data, trueUrl) })
	}

	err = errGroup.Wait()
	if err != nil {
		fmt.Printf("Error downloading file: %s\n", err)
		return nil, -1, fmt.Errorf("error downloading file: %w", err)
	}
	elapsed := time.Since(startTime).Seconds()
	througput := humanize.Bytes(uint64(float64(fileSize) / elapsed))
	fmt.Printf("Downloaded %s bytes in %.3fs (%s/s)\n", humanize.Bytes(uint64(fileSize)), elapsed, througput)

	buffer := bytes.NewBuffer(data)
	return buffer, fileSize, nil
}

func downloadChunk(start, end int64, data []byte, trueUrl string) error {
	success := false

	client := newClient()

	req, err := http.NewRequest("GET", trueUrl, nil)
	if err != nil {
		// This needs to be a time.Duration to make everything happy
		return fmt.Errorf("error creating request for %s: %w", req.URL.String(), err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download %s: %w", req.URL.String(), err)

	}
	defer resp.Body.Close()

	n, err := io.ReadFull(resp.Body, data[start:end+1])
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading response for %s: %w", req.URL.String(), err)

	}
	if n != int(end-start+1) {
		return fmt.Errorf("downloaded %d bytes instead of %d for %s", n, end-start+1, req.URL.String())
	}
	success = true

	if !success {
		return fmt.Errorf("failed to download after %d retries", viper.GetInt("retries"))
	}
	return nil
}

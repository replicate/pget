package download

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"

	"github.com/replicate/pget/optname"
)

var (
	fileSize int64
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

	fSize := resp.ContentLength
	if fSize <= 0 {
		return "", int64(-1), fmt.Errorf("unable to determine file size")
	}
	fileSize = fSize
	return trueUrl, fileSize, nil
}

func DownloadFileToBuffer(url string) (*bytes.Buffer, int64, error) {
	maxConcurrency := viper.GetInt(optname.Concurrency)

	trueUrl, fileSize, err := getRemoteFileSize(url)
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

	var wg sync.WaitGroup
	wg.Add(concurrency)

	data := make([]byte, fileSize)
	errc := make(chan error, concurrency)
	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1

		if i == concurrency-1 {
			end = fileSize - 1
		}

		go func(start, end int64) {
			defer wg.Done()
			client := newClient()

			req, err := http.NewRequest("GET", trueUrl, nil)
			if err != nil {
				fmt.Printf("Error creating request: %v\n", err)
				errc <- fmt.Errorf("failed to download %s", req.URL.String())
				return
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("Error executing request: %v\n", err)
				errc <- fmt.Errorf("error executing request for %s: %w", req.URL.String(), err)
				return
			}
			defer resp.Body.Close()

			n, err := io.ReadFull(resp.Body, data[start:end+1])
			if err != nil && err != io.EOF {
				fmt.Printf("Error reading response: %v\n", err)
				errc <- fmt.Errorf("error reading response for %s: %w", req.URL.String(), err)
			}
			if n != int(end-start+1) {
				fmt.Printf("Downloaded %d bytes instead of %d\n", n, end-start+1)
				errc <- fmt.Errorf("downloaded %d bytes instead of %d for %s", n, end-start+1, req.URL.String())
			}
		}(start, end)
	}

	wg.Wait()
	close(errc) // close the error channel
	for err := range errc {
		if err != nil {
			return nil, -1, err // return the first error we encounter
		}
	}
	elapsed := time.Since(startTime).Seconds()
	througput := humanize.Bytes(uint64(float64(fileSize) / elapsed))
	fmt.Printf("Downloaded %s bytes in %.3fs (%s/s)\n", humanize.Bytes(uint64(fileSize)), elapsed, througput)

	buffer := bytes.NewBuffer(data)
	return buffer, fileSize, nil
}

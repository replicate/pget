package download

import (
	"bytes"
	"fmt"
	"github.com/replicate/pget/optname"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
)

const (
	retryDelayBaseline = 100 // in milliseconds
	retrySleepJitter   = 500 // in milliseconds (will add 0-500 additional milliseconds)

	retryMaxBackoffTime = 3000 // in milliseconds, do not backoff further than 3 seconds
	retryBackoffIncr    = 500  // in milliseconds, backoffFactor^retryNum * backoffIncr
	retryBackoffFactor  = 2    // Base for POW()
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
	verboseMode := viper.GetBool(optname.Verbose)
	maxConcurrency := viper.GetInt(optname.Concurrency)
	retries := viper.GetInt(optname.Retries)

	trueUrl, fileSize, err := getRemoteFileSize(url)
	if err != nil {
		return nil, -1, err
	}

	chunkSize := viper.GetInt64(optname.MinimumChunkSize)
	// not more than one connection per min chunk size
	concurrency := int(math.Ceil(float64(fileSize) / float64(chunkSize)))

	if concurrency > maxConcurrency {
		concurrency = maxConcurrency
		chunkSize = fileSize / int64(concurrency)
	}
	if verboseMode {
		fmt.Printf("Downloading %s bytes with %d connections (chunk-size = %d)\n", humanize.Bytes(uint64(fileSize)), concurrency, humanize.Bytes(uint64(chunkSize)))
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

			success := false
			for retryNum := 0; retryNum <= retries; retryNum++ {

				if retryNum > 0 {
					if verboseMode {
						fmt.Printf("Retrying. Count: %d\n", retryNum)
					}
					sleepJitter := time.Duration(rand.Intn(retrySleepJitter))
					sleepTime := time.Millisecond * (sleepJitter + retryDelayBaseline)

					// Exponential backoff
					// 2^retryNum * retryBackoffIncr (in milliseconds)
					backoffFactor := math.Pow(retryBackoffFactor, float64(retryNum))
					backoffDuration := time.Duration(math.Min(backoffFactor*retryBackoffIncr, retryMaxBackoffTime))
					sleepTime += time.Millisecond * backoffDuration
					time.Sleep(sleepTime)
				}

				transport := http.DefaultTransport.(*http.Transport).Clone()
				transport.DisableKeepAlives = true
				checkRedirectFunc := func(req *http.Request, via []*http.Request) error {
					if verboseMode {
						fmt.Printf("Received redirect '%d' to '%s'\n", req.Response.StatusCode, req.URL.String())
					}
					return nil
				}
				client := &http.Client{
					Transport:     transport,
					CheckRedirect: checkRedirectFunc,
				}

				req, err := http.NewRequest("GET", trueUrl, nil)
				if err != nil {
					// This needs to be a time.Duration to make everything happy
					fmt.Printf("Error creating request: %v\n", err)
					continue
				}
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
				if retryNum > 0 {
					req.Header.Set("Retry-Count", fmt.Sprintf("%d", retryNum))
				}

				resp, err := client.Do(req)
				if err != nil {
					fmt.Printf("Error executing request: %v\n", err)
					continue
				}
				defer resp.Body.Close()

				n, err := io.ReadFull(resp.Body, data[start:end+1])
				if err != nil && err != io.EOF {
					fmt.Printf("Error reading response: %v\n", err)
					continue
				}
				if n != int(end-start+1) {
					fmt.Printf("Downloaded %d bytes instead of %d\n", n, end-start+1)
					continue
				}
				success = true
				break // if the download was successful, break out of the retry loop
			}

			if !success {
				errc <- fmt.Errorf("failed to download after %d retries", retries)
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

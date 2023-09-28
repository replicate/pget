package main

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

const (
	retryDelayBaseline = 100 // in milliseconds
	retrySleepJitter   = 500 // in milliseconds (will add 0-500 additional milliseconds)

	retryMaxBackoffTime = 3000 // in milliseconds, we will not backoff further than 3 seconds
	retryBackoffIncr    = 500  // in milliseconds, backoffFactor^retryNum * backoffIncr
	retryBackoffFactor  = 2    // Base for POW()
)

var (
	_fileSize   int64
	verboseMode bool = false
)

func getRemoteFileSize(url string) (string, int64, error) {
	// TODO: this needs a retry
	resp, err := http.DefaultClient.Head(url)
	if err != nil {
		return "", int64(-1), err
	}
	defer resp.Body.Close()
	trueUrl := resp.Request.URL.String()
	if (trueUrl != url) {
		fmt.Printf("Redirected to %s\n", trueUrl)
	}

	fileSize := resp.ContentLength
	if fileSize <= 0 {
		return "", int64(-1), fmt.Errorf("unable to determine file size")
	}
	_fileSize = fileSize
	return trueUrl, fileSize, nil
}

func downloadFileToBuffer(url string, concurrency int, retries int) (*bytes.Buffer, error) {
	trueUrl, fileSize, err := getRemoteFileSize(url)
	if err != nil {
		return nil, err
	}

	chunkSize := fileSize / int64(concurrency)
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
					sleepTime += (time.Millisecond * backoffDuration)
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
			return nil, err // return the first error we encounter
		}
	}
	elapsed := time.Since(startTime).Seconds()
	througput := humanize.Bytes(uint64(float64(fileSize) / elapsed))
	fmt.Printf("Downloaded %s bytes in %.3fs (%s/s)\n", humanize.Bytes(uint64(fileSize)), elapsed, througput)

	buffer := bytes.NewBuffer(data)
	return buffer, nil
}

func extractTarFile(buffer *bytes.Buffer, destDir string) error {
	startTime := time.Now()
	tarReader := tar.NewReader(buffer)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, header.Name)
		targetDir := filepath.Dir(target)
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			targetFile, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(targetFile, tarReader); err != nil {
				targetFile.Close()
				return err
			}
			targetFile.Close()
		case tar.TypeSymlink:
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported file type for %s, typeflag %s", header.Name, string(header.Typeflag))
		}
	}
	elapsed := time.Since(startTime).Seconds()
	size := humanize.Bytes(uint64(_fileSize))
	throughput := humanize.Bytes(uint64(float64(_fileSize) / elapsed))
	fmt.Printf("Extracted %s in %.3fs (%s/s)\n", size, elapsed, throughput)

	return nil
}

func main() {
	// define flags
	concurrency := flag.Int("c", runtime.GOMAXPROCS(0)*4, "concurrency level - default 4 * cores")
	retries := flag.Int("r", 5, "Number of retries when attempting to retreive file")
	extract := flag.Bool("x", false, "extract tar file")
	verbose := flag.Bool("v", false, "verbose mode")
	force := flag.Bool("f", false, "force download, overwriting existing file")
	flag.Parse()

	// check required positional arguments
	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("Usage: pcurl <url> <dest> [-c concurrency] [-r max-retries] [-v] [-x]")
		os.Exit(1)
	}

	url := args[0]
	dest := args[1]

	// ensure dest does not exist
	if _, err := os.Stat(dest); !*force || !os.IsNotExist(err) {
		fmt.Printf("Destination %s already exists\n", dest)
		os.Exit(1)
	}

	if *verbose {
		verboseMode = true
	}

	buffer, err := downloadFileToBuffer(url, *concurrency, *retries)
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		os.Exit(1)
	}

	// extract the tar file if the -x flag was provided
	if *extract {
		err = extractTarFile(buffer, dest)
		if err != nil {
			fmt.Printf("Error extracting tar file: %v\n", err)
			os.Exit(1)
		}
	} else {
		// if -x flag is not set, save the buffer to a file
		err = ioutil.WriteFile(dest, buffer.Bytes(), 0644)
		if err != nil {
			fmt.Printf("Error writing file: %v\n", err)
			os.Exit(1)
		}
	}

}

package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/replicate/pget/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	retryDelayBaseline = 100 // in milliseconds
	retrySleepJitter   = 500 // in milliseconds (will add 0-500 additional milliseconds)

	retryMaxBackoffTime = 3000 // in milliseconds, we will not backoff further than 3 seconds
	retryBackoffIncr    = 500  // in milliseconds, backoffFactor^retryNum * backoffIncr
	retryBackoffFactor  = 2    // Base for POW()
)

var (
	_fileSize int64
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
	_fileSize = fileSize
	return trueUrl, fileSize, nil
}

func downloadFileToBuffer(url string, maxConcurrency int, retries int) (*bytes.Buffer, error) {
	verboseMode := viper.GetBool("verbose")
	trueUrl, fileSize, err := getRemoteFileSize(url)
	if err != nil {
		return nil, err
	}

	chunkSize := viper.GetInt64("target-chunk-size")
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
	cmd := &cobra.Command{
		Use:   "pget [flags] <url> <dest>",
		Short: "pget",
		Long:  `Parallel file downloader`,
		Run:   mainFunc,
	}
	config.AddFlags(cmd)
	cmd.Execute()
}

func mainFunc(cmd *cobra.Command, args []string) {
	// check required positional arguments
	verboseMode := viper.GetBool("verbose")
	useRemoteName := viper.GetBool("remote-name")
	if useRemoteName && len(args) < 1 || !useRemoteName && len(args) < 2 {
		cmd.Usage()
		os.Exit(1)
	} else if useRemoteName && len(args) > 1 {
		fmt.Println("`-O` flag cannot be used with positional <dest> (second) argument.")
		cmd.Usage()
		os.Exit(1)
	}

	if useRemoteName {
		urlParts, err := url.Parse(args[0])
		if err != nil {
			fmt.Printf("Error parsing URL: %v\n", err)
			os.Exit(1)
		}
		fileName := filepath.Base(urlParts.Path)
		args = append(args, fileName)
	}
	url := args[0]
	dest := args[1]
	_, fileExists := os.Stat(dest)

	if verboseMode {
		absPath, _ := filepath.Abs(dest)
		fmt.Println("URL:", url)
		fmt.Println("Destination:", absPath)
		fmt.Println("Target Chunk Size:", humanize.Bytes(uint64(viper.GetInt64("target-chunk-size"))))
		fmt.Println()
	}
	// ensure dest does not exist
	if !viper.GetBool("force") && !os.IsNotExist(fileExists) {
		fmt.Printf("Destination %s already exists\n", dest)
		os.Exit(1)
	}

	// allows us to see how many pget procs are running at a time
	tmpFile := fmt.Sprintf("/tmp/.pget-%d", os.Getpid())
	os.WriteFile(tmpFile, []byte(""), 0644)
	defer os.Remove(tmpFile)

	buffer, err := downloadFileToBuffer(url, viper.GetInt("concurrency"), viper.GetInt("retries"))
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		os.Exit(1)
	}

	// extract the tar file if the -x flag was provided
	if viper.GetBool("extract") {
		err = extractTarFile(buffer, dest)
		if err != nil {
			fmt.Printf("Error extracting tar file: %v\n", err)
			os.Exit(1)
		}
	} else {
		// if -x flag is not set, save the buffer to a file
		err = os.WriteFile(dest, buffer.Bytes(), 0644)
		if err != nil {
			fmt.Printf("Error writing file: %v\n", err)
			os.Exit(1)
		}
	}
}

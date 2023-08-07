package main

import (
	"archive/tar"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

var _fileSize int64

func getRemoteFileSize(url string) (int64, error) {
	resp, err := http.DefaultClient.Head(url)
	if err != nil {
		return int64(-1), err
	}
	defer resp.Body.Close()

	fileSize := resp.ContentLength
	if fileSize <= 0 {
		return int64(-1), fmt.Errorf("unable to determine file size")
	}
	_fileSize = fileSize
	return fileSize, nil
}

func downloadFile(url string, destFile *os.File, concurrency int) error {
	fileSize, err := getRemoteFileSize(url)
	if err != nil {
		return err
	}

	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		os.Exit(1)
	}

	err = destFile.Truncate(fileSize)
	if err != nil {
		return err
	}

	chunkSize := fileSize / int64(concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)

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
			fh, err := os.OpenFile(destFile.Name(), os.O_RDWR, 0644)
			if err != nil {
				errc <- fmt.Errorf("Failed to reopen file: %v", err)
			}
			defer fh.Close()

			retries := 5
			for retries > 0 {
				transport := http.DefaultTransport.(*http.Transport).Clone()
				transport.DisableKeepAlives = true
				client := &http.Client{
					Transport: transport,
				}

				req, err := http.NewRequest("GET", url, nil)
				if err != nil {
					fmt.Printf("Error creating request: %v\n", err)
					retries--
					time.Sleep(time.Millisecond * 100) // wait 100 milliseconds before retrying
					continue
				}
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

				resp, err := client.Do(req)
				if err != nil {
					fmt.Printf("Error executing request: %v\n", err)
					retries--
					time.Sleep(time.Millisecond * 100) // wait 100 milliseconds before retrying
					continue
				}
				defer resp.Body.Close()

				_, err = fh.Seek(start, 0)
				if err != nil {
					fmt.Printf("Error seeking in file: %v\n", err)
					retries--
					time.Sleep(time.Millisecond * 100) // wait 100 milliseconds before retrying
					continue
				}

				n, err := io.CopyN(fh, resp.Body, end-start+1)
				if err != nil && err != io.EOF {
					fmt.Printf("Error reading response: %v\n", err)
					retries--
					time.Sleep(time.Millisecond * 100) // wait 100 milliseconds before retrying
					continue
				}
				if n != end-start+1 {
					fmt.Printf("Downloaded %d bytes instead of %d\n", n, end-start+1)
					retries--
					time.Sleep(time.Millisecond * 100) // wait 100 milliseconds before retrying
					continue
				}
				break // if the download was successful, break out of the retry loop
			}

			if retries == 0 {
				errc <- fmt.Errorf("failed to download after multiple retries")

			}
		}(start, end)
	}

	wg.Wait()
	close(errc) // close the error channel
	for err := range errc {
		if err != nil {
			return err // return the first error we encounter
		}
	}
	elapsed := time.Since(startTime).Seconds()
	througput := humanize.Bytes(uint64(float64(fileSize) / elapsed))
	fmt.Printf("Downloaded %s bytes in %.3fs (%s/s)\n", humanize.Bytes(uint64(fileSize)), elapsed, througput)

	return nil
}

func extractTarFile(input io.Reader, destDir string) error {
	startTime := time.Now()
	tarReader := tar.NewReader(input)

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
	extract := flag.Bool("x", false, "extract tar file")
	flag.Parse()

	// check required positional arguments
	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("Usage: pcurl [-c concurrency] [-x] <url> <dest>")
		os.Exit(1)
	}

	url := args[0]
	dest := args[1]

	// ensure dest does not exist
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		fmt.Printf("Destination %s already exists\n", dest)
		os.Exit(1)
	}

	// create tempfile for downloading to
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting cwd: %v\n", err)
		os.Exit(1)
	}
	destTemp, err := os.CreateTemp(cwd, dest+".partial")
	if err != nil {
		fmt.Printf("Failed to create temp file: %v\n", err)
		os.Exit(1)
	}

	err = downloadFile(url, destTemp, *concurrency)
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		os.Exit(1)
	}

	// extract the tar file if the -x flag was provided
	if *extract {
		_, err = destTemp.Seek(0, 0)
		if err != nil {
			fmt.Printf("Error extracting tar file: %v\n", err)
			os.Exit(1)
		}
		err = extractTarFile(destTemp, dest)
		if err != nil {
			fmt.Printf("Error extracting tar file: %v\n", err)
			os.Exit(1)
		}
		destTemp.Close()
		os.Remove(destTemp.Name())
	} else {
		// move destTemp to dest
		err = os.Rename(destTemp.Name(), dest)
		if err != nil {
			fmt.Printf("Error moving downloaded file to correct location: %v\n", err)
			os.Exit(1)
		}
	}
}

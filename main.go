package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

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
	return fileSize, nil
}

func downloadFileToBuffer(url string, concurrency int) (*bytes.Buffer, error) {
	fileSize, err := getRemoteFileSize(url)
	if err != nil {
		return nil, err
	}

	chunkSize := fileSize / int64(concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)

	data := make([]byte, fileSize)
	errc := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1

		if i == concurrency-1 {
			end = fileSize - 1
		}

		go func(start, end int64) {
			defer wg.Done()

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

				n, err := io.ReadFull(resp.Body, data[start:end+1])
				if err != nil && err != io.EOF {
					fmt.Printf("Error reading response: %v\n", err)
					retries--
					time.Sleep(time.Millisecond * 100) // wait 100 milliseconds before retrying
					continue
				}
				if n != int(end-start+1) {
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
			return nil, err // return the first error we encounter
		}
	}

	buffer := bytes.NewBuffer(data)
	return buffer, nil
}

func extractTarFile(buffer *bytes.Buffer, destDir string) error {
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
		default:
			return fmt.Errorf("unsupported file type for %s", header.Name)
		}
	}

	return nil
}

func main() {
	args := os.Args[1:]
	if len(args) < 3 {
		fmt.Println("Usage: pcurl <url> <dest> <concurrency>")
		os.Exit(1)
	}

	url := args[0]
	dest := args[1]

	// ensure dest does not exist
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		fmt.Printf("Destination %s already exists\n", dest)
		os.Exit(1)
	}

	concurrency, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Println("Invalid concurrency value")
		os.Exit(1)
	}

	buffer, err := downloadFileToBuffer(url, concurrency)
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		os.Exit(1)
	}

	err = extractTarFile(buffer, dest)
	if err != nil {
		fmt.Printf("Error extracting tar file: %v\n", err)
		os.Exit(1)
	}
}

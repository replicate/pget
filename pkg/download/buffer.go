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
	"regexp"
	"runtime"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/logging"
)

const BufferModeName = "buffer"
const defaultMinChunkSize = 16 * 1024 * 1024

var contentRangeRegexp = regexp.MustCompile(`^bytes .*/([0-9]+)$`)

type BufferMode struct {
	Client *client.HTTPClient
	Options
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

func GetBufferMode(opts Options) Mode {
	client := client.NewHTTPClient(opts.Client)
	return &BufferMode{
		Client:  client,
		Options: opts,
	}
}

func (m *BufferMode) maxChunks() int {
	maxChunks := m.MaxChunks
	if maxChunks == 0 {
		return runtime.NumCPU() * 4
	}
	return maxChunks
}

func (m *BufferMode) minChunkSize() int64 {
	minChunkSize := m.MinChunkSize
	if minChunkSize == 0 {
		return defaultMinChunkSize
	}
	return minChunkSize
}

func (m *BufferMode) getFileSizeFromContentRange(contentRange string) (int64, error) {
	groups := contentRangeRegexp.FindStringSubmatch(contentRange)
	if groups == nil {
		return -1, fmt.Errorf("Couldn't parse Content-Range: %s", contentRange)
	}
	return strconv.ParseInt(groups[1], 10, 64)
}

func (m *BufferMode) fileToBuffer(ctx context.Context, target Target) (*bytes.Buffer, int64, error) {
	logger := logging.GetLogger()

	firstChunkResp, err := m.doRequest(ctx, 0, m.minChunkSize()-1, target)
	if err != nil {
		return nil, -1, err
	}

	trueURL := firstChunkResp.Request.URL.String()
	if trueURL != target.URL {
		logger.Info().Str("url", target.URL).Str("redirect_url", trueURL).Msg("Redirect")
		target.TrueURL = trueURL
	}

	fileSize, err := m.getFileSizeFromContentRange(firstChunkResp.Header.Get("Content-Range"))
	if err != nil {
		firstChunkResp.Body.Close()
		return nil, -1, err
	}

	data := make([]byte, fileSize)
	if fileSize < m.minChunkSize() {
		// we only need a single chunk: just download it and finish
		err = m.downloadChunk(firstChunkResp, data[0:fileSize])
		if err != nil {
			return nil, -1, err
		}
		return bytes.NewBuffer(data), fileSize, nil
	}

	chunkSize := (fileSize - m.minChunkSize()) / int64(m.maxChunks()-1)
	if chunkSize < m.minChunkSize() {
		chunkSize = m.minChunkSize()
	}
	if chunkSize < 0 {
		firstChunkResp.Body.Close()
		return nil, -1, fmt.Errorf("error: chunksize incorrect - result is negative, %d", chunkSize)
	}
	// not more than one connection per min chunk size
	chunks := int(math.Ceil(float64(fileSize) / float64(chunkSize)))

	if chunks > m.maxChunks() {
		chunks = m.maxChunks()
		chunkSize = fileSize / int64(chunks)
	}
	logger.Debug().Str("dest", target.Dest).
		Str("url", target.URL).
		Int64("size", fileSize).
		Int("connections", chunks).
		Int64("chunkSize", chunkSize).
		Msg("Downloading")

	errGroup, ctx := errgroup.WithContext(ctx)

	startTime := time.Now()

	errGroup.Go(func() error {
		return m.downloadChunk(firstChunkResp, data[0:m.minChunkSize()])
	})

	for i := 1; i < chunks; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1

		if i == chunks-1 {
			end = fileSize - 1
		}
		resp, err := m.doRequest(ctx, start, end, target)
		if err != nil {
			return nil, -1, err
		}
		errGroup.Go(func() error {
			return m.downloadChunk(resp, data[start:end+1])
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, -1, err // return the first error we encounter
	}

	elapsed := time.Since(startTime)
	througput := fmt.Sprintf("%s/s", humanize.Bytes(uint64(float64(fileSize)/elapsed.Seconds())))
	logger.Info().Str("url", target.URL).
		Str("dest", target.Dest).
		Str("size", humanize.Bytes(uint64(fileSize))).
		Str("elapsed", fmt.Sprintf("%.3fs", elapsed.Seconds())).
		Str("throughput", througput).
		Msg("Complete")

	buffer := bytes.NewBuffer(data)
	return buffer, fileSize, nil
}

func (m *BufferMode) doRequest(ctx context.Context, start, end int64, target Target) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", target.TrueURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s: %w", req.URL.String(), err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := m.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request for %s: %w", req.URL.String(), err)
	}
	if resp.StatusCode == 0 || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected http status downloading %s: %s", req.URL.String(), resp.Status)
	}

	return resp, nil
}

func (m *BufferMode) downloadChunk(resp *http.Response, dataSlice []byte) error {
	defer resp.Body.Close()
	expectedBytes := len(dataSlice)
	n, err := io.ReadFull(resp.Body, dataSlice)
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading response for %s: %w", resp.Request.URL.String(), err)
	}
	if n != expectedBytes {
		return fmt.Errorf("downloaded %d bytes instead of %d for %s", n, expectedBytes, resp.Request.URL.String())
	}
	return nil
}

func (m *BufferMode) DownloadFile(ctx context.Context, url string, dest string) (int64, time.Duration, error) {
	logger := logging.GetLogger()
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
	logger.Debug().
		Str("dest", dest).
		Str("elapsed", fmt.Sprintf("%.3fs", writeElapsed.Seconds())).
		Msg("Write Complete")
	return fileSize, downloadCompleteDuration, nil
}

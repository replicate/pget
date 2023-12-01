package download

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"runtime"
	"strconv"

	"golang.org/x/sync/errgroup"

	"github.com/dustin/go-humanize"
	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/logging"
)

const defaultMinChunkSize = 16 * humanize.MiByte

var contentRangeRegexp = regexp.MustCompile(`^bytes .*/([0-9]+)$`)

type BufferMode struct {
	Client *client.HTTPClient
	Options
}

func GetBufferMode(opts Options) *BufferMode {
	client := client.NewHTTPClient(opts.Client)
	return &BufferMode{
		Client:  client,
		Options: opts,
	}
}

func (m *BufferMode) maxChunks() int {
	maxChunks := m.MaxConcurrency
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
		return -1, fmt.Errorf("couldn't parse Content-Range: %s", contentRange)
	}
	return strconv.ParseInt(groups[1], 10, 64)
}

func (m *BufferMode) fileToBuffer(ctx context.Context, url string) (*bytes.Buffer, int64, error) {
	logger := logging.GetLogger()

	firstChunkResp, err := m.doRequest(ctx, 0, m.minChunkSize()-1, url)
	if err != nil {
		return nil, -1, err
	}

	trueURL := firstChunkResp.Request.URL.String()
	if trueURL != url {
		logger.Info().Str("url", url).Str("redirect_url", trueURL).Msg("Redirect")
	}

	fileSize, err := m.getFileSizeFromContentRange(firstChunkResp.Header.Get("Content-Range"))
	if err != nil {
		firstChunkResp.Body.Close()
		return nil, -1, err
	}

	data := make([]byte, fileSize)
	if fileSize <= m.minChunkSize() {
		// we only need a single chunk: just download it and finish
		err = m.downloadChunk(firstChunkResp, data[0:fileSize])
		if err != nil {
			return nil, -1, err
		}
		return bytes.NewBuffer(data), fileSize, nil
	}

	remainingBytes := fileSize - m.minChunkSize()
	numChunks := int(remainingBytes / m.minChunkSize())
	// Number of chunks can never be 0
	if numChunks <= 0 {
		numChunks = 1
	}
	if numChunks > m.maxChunks() {
		numChunks = m.maxChunks()
	}

	chunkSize := remainingBytes / int64(numChunks)
	if chunkSize < 0 {
		firstChunkResp.Body.Close()
		return nil, -1, fmt.Errorf("error: chunksize incorrect - result is negative, %d", chunkSize)
	}

	logger.Debug().Str("url", url).
		Int64("size", fileSize).
		Int("connections", numChunks).
		Int64("chunkSize", chunkSize).
		Msg("Downloading")

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		return m.downloadChunk(firstChunkResp, data[0:m.minChunkSize()])
	})

	startOffset := m.minChunkSize()

	for i := 0; i < numChunks; i++ {
		start := startOffset + chunkSize*int64(i)
		end := start + chunkSize - 1

		if i == numChunks-1 {
			end = fileSize - 1
		}
		resp, err := m.doRequest(ctx, start, end, trueURL)
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

	buffer := bytes.NewBuffer(data)
	return buffer, fileSize, nil
}

func (m *BufferMode) doRequest(ctx context.Context, start, end int64, trueURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", trueURL, nil)
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

func (m *BufferMode) Fetch(ctx context.Context, url string) (result io.Reader, fileSize int64, err error) {
	buffer, fileSize, err := m.fileToBuffer(ctx, url)
	if err != nil {
		return nil, 0, err
	}
	return buffer, fileSize, nil
}

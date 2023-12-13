package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
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
	eg *errgroup.Group
}

func GetBufferMode(opts Options) *BufferMode {
	eg := new(errgroup.Group)
	client := client.NewHTTPClient(opts.Client)
	eg.SetLimit(opts.maxConcurrency())
	return &BufferMode{
		Client:  client,
		Options: opts,
		eg:      eg,
	}
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

type firstReqResult struct {
	fileSize int64
	trueURL  string
	err      error
}

func (m *BufferMode) Fetch(ctx context.Context, url string) (io.Reader, int64, error) {
	logger := logging.GetLogger()

	br := newBufferedReader(m.minChunkSize())

	firstReqResultChan := make(chan firstReqResult)
	m.eg.Go(func() error {
		defer close(firstReqResultChan)
		firstChunkResp, err := m.DoRequest(ctx, 0, m.minChunkSize()-1, url)
		if err != nil {
			firstReqResultChan <- firstReqResult{err: err}
			return err
		}

		defer firstChunkResp.Body.Close()

		trueURL := firstChunkResp.Request.URL.String()
		if trueURL != url {
			logger.Info().Str("url", url).Str("redirect_url", trueURL).Msg("Redirect")
		}

		fileSize, err := m.getFileSizeFromContentRange(firstChunkResp.Header.Get("Content-Range"))
		if err != nil {
			firstReqResultChan <- firstReqResult{err: err}
			return err
		}
		firstReqResultChan <- firstReqResult{fileSize: fileSize, trueURL: trueURL}

		return br.downloadBody(firstChunkResp)
	})

	firstReqResult, ok := <-firstReqResultChan
	if !ok {
		return nil, -1, fmt.Errorf("Logic error: channel closed but no result received")
	}

	if firstReqResult.err != nil {
		return nil, -1, firstReqResult.err
	}

	fileSize := firstReqResult.fileSize
	trueURL := firstReqResult.trueURL

	if fileSize <= m.minChunkSize() {
		// we only need a single chunk: just download it and finish
		return br, fileSize, nil
	}

	remainingBytes := fileSize - m.minChunkSize()
	numChunks := int(remainingBytes / m.minChunkSize())
	// Number of chunks can never be 0
	if numChunks <= 0 {
		numChunks = 1
	}
	if numChunks > m.maxConcurrency() {
		numChunks = m.maxConcurrency()
	}

	chunkReaders := make([]io.Reader, numChunks+1)

	chunkReaders[0] = br

	startOffset := m.minChunkSize()

	chunkSize := remainingBytes / int64(numChunks)
	if chunkSize < 0 {
		return nil, -1, fmt.Errorf("error: chunksize incorrect - result is negative, %d", chunkSize)
	}

	logger.Debug().Str("url", url).
		Int64("size", fileSize).
		Int("connections", numChunks).
		Int64("chunkSize", chunkSize).
		Msg("Downloading")

	for i := 0; i < numChunks; i++ {
		start := startOffset + chunkSize*int64(i)
		end := start + chunkSize - 1

		if i == numChunks-1 {
			end = fileSize - 1
		}

		br := newBufferedReader(end - start + 1)
		chunkReaders[i+1] = br

		m.eg.Go(func() error {
			resp, err := m.DoRequest(ctx, start, end, trueURL)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			return br.downloadBody(resp)
		})
	}

	return io.MultiReader(chunkReaders...), fileSize, nil
}

func (m *BufferMode) DoRequest(ctx context.Context, start, end int64, trueURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", trueURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s: %w", trueURL, err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := m.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request for %s: %w", req.URL.String(), err)
	}
	if resp.StatusCode == 0 || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w %s: %s", ErrUnexpectedHTTPStatus, req.URL.String(), resp.Status)
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

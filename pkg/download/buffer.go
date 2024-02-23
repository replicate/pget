package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"

	"golang.org/x/sync/errgroup"

	"github.com/dustin/go-humanize"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/logging"
	"github.com/replicate/pget/pkg/multireader"
)

const defaultMinChunkSize = 16 * humanize.MiByte

var contentRangeRegexp = regexp.MustCompile(`^bytes .*/([0-9]+)$`)

type BufferMode struct {
	Client *client.HTTPClient
	Options

	// we use this errgroup as a semaphore (via sem.SetLimit())
	sem   *errgroup.Group
	queue *workQueue
}

func GetBufferMode(opts Options) *BufferMode {
	client := client.NewHTTPClient(opts.Client)
	sem := new(errgroup.Group)
	sem.SetLimit(opts.maxConcurrency())
	queue := newWorkQueue(opts.maxConcurrency())
	queue.start()
	return &BufferMode{
		Client:  client,
		Options: opts,
		sem:     sem,
		queue:   queue,
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

func readBody(reader *multireader.BufferedReader, resp *http.Response) error {
	expectedBytes := resp.ContentLength
	_ = reader.SetSize(expectedBytes)
	n, err := reader.ReadFrom(resp.Body)
	if errors.Is(err, io.EOF) {
		reader.Err = fmt.Errorf("error reading response for %s: %w", resp.Request.URL.String(), err)
		return reader.Err
	}
	if n != expectedBytes {
		reader.Err = fmt.Errorf("downloaded %d bytes instead of %d for %s", n, expectedBytes, resp.Request.URL.String())
		return reader.Err
	}
	return nil
}

func (m *BufferMode) Fetch(ctx context.Context, url string) (io.Reader, int64, error) {
	logger := logging.GetLogger()

	br := multireader.NewBufferedReader(m.minChunkSize())

	firstReqResultCh := make(chan firstReqResult)
	m.queue.submit(func() {
		m.sem.Go(func() error {
			defer close(firstReqResultCh)
			defer br.Done()
			firstChunkResp, err := m.DoRequest(ctx, 0, m.minChunkSize()-1, url)
			if err != nil {
				br.Err = err
				firstReqResultCh <- firstReqResult{err: err}
				return err
			}

			defer firstChunkResp.Body.Close()

			trueURL := firstChunkResp.Request.URL.String()
			if trueURL != url {
				logger.Info().Str("url", url).Str("redirect_url", trueURL).Msg("Redirect")
			}

			fileSize, err := m.getFileSizeFromContentRange(firstChunkResp.Header.Get("Content-Range"))
			if err != nil {
				firstReqResultCh <- firstReqResult{err: err}
				return err
			}
			firstReqResultCh <- firstReqResult{fileSize: fileSize, trueURL: trueURL}
			return readBody(br, firstChunkResp)
		})
	})

	firstReqResult, ok := <-firstReqResultCh
	if !ok {
		panic("logic error in BufferMode: first request didn't return any output")
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

	readersCh := make(chan *multireader.BufferedReader, m.maxConcurrency()+1)
	readersCh <- br

	startOffset := m.minChunkSize()

	chunkSize := remainingBytes / int64(numChunks)
	if chunkSize < 0 {
		return nil, -1, fmt.Errorf("error: chunksize incorrect - result is negative, %d", chunkSize)
	}

	m.queue.submit(func() {
		defer close(readersCh)
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

			br := multireader.NewBufferedReader(end - start + 1)
			readersCh <- br

			m.sem.Go(func() error {
				defer br.Done()
				resp, err := m.DoRequest(ctx, start, end, trueURL)
				if err != nil {
					br.Err = err
					return err
				}
				defer resp.Body.Close()
				return readBody(br, resp)
			})
		}
	})

	return multireader.NewMultiReader(readersCh), fileSize, nil
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

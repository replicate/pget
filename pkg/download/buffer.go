package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/logging"
)

type BufferMode struct {
	Client *client.HTTPClient
	Options

	// we use this errgroup as a semaphore (via sem.SetLimit())
	sem   *errgroup.Group
	queue *workQueue

	bufferedReaderPool *readerPool
}

func GetBufferMode(opts Options) *BufferMode {
	client := client.NewHTTPClient(opts.Client)
	sem := new(errgroup.Group)
	sem.SetLimit(opts.maxConcurrency())
	queue := newWorkQueue(opts.maxConcurrency())
	queue.start()
	mode := &BufferMode{
		Client:  client,
		Options: opts,
		sem:     sem,
		queue:   queue,
	}
	mode.bufferedReaderPool = newReaderPool(mode.chunkSize())
	return mode
}

func (m *BufferMode) chunkSize() int64 {
	minChunkSize := m.ChunkSize
	if minChunkSize == 0 {
		return defaultChunkSize
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

	br := m.bufferedReaderPool.Get()

	firstReqResultCh := make(chan firstReqResult)
	m.queue.submit(func() {
		m.sem.Go(func() error {
			defer close(firstReqResultCh)
			defer br.done()
			firstChunkResp, err := m.DoRequest(ctx, 0, m.chunkSize()-1, url)
			if err != nil {
				br.err = err
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

			return br.downloadBody(firstChunkResp)
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

	if fileSize <= m.chunkSize() {
		// we only need a single chunk: just download it and finish
		return br, fileSize, nil
	}

	remainingBytes := fileSize - m.chunkSize()
	// integer divide rounding up
	numChunks := int((remainingBytes-1)/m.chunkSize() + 1)

	readersCh := make(chan io.Reader, numChunks+1)
	readersCh <- br

	startOffset := m.chunkSize()

	m.queue.submit(func() {
		defer close(readersCh)
		logger.Debug().Str("url", url).
			Int64("size", fileSize).
			Int("connections", numChunks).
			Int64("chunkSize", m.chunkSize()).
			Msg("Downloading")

		for i := 0; i < numChunks; i++ {
			start := startOffset + m.chunkSize()*int64(i)
			end := start + m.chunkSize() - 1

			if i == numChunks-1 {
				end = fileSize - 1
			}

			br := m.bufferedReaderPool.Get()
			readersCh <- br

			m.sem.Go(func() error {
				defer br.done()
				resp, err := m.DoRequest(ctx, start, end, trueURL)
				if err != nil {
					br.err = err
					return err
				}
				defer resp.Body.Close()
				return br.downloadBody(resp)
			})
		}
	})

	return newChanMultiReader(readersCh), fileSize, nil
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

package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/logging"
)

type BufferMode struct {
	Client client.HTTPClient
	Options

	queue *priorityWorkQueue
}

func GetBufferMode(opts Options) *BufferMode {
	client := client.NewHTTPClient(opts.Client)
	m := &BufferMode{
		Client:  client,
		Options: opts,
	}
	m.queue = newWorkQueue(opts.maxConcurrency(), m.chunkSize())
	m.queue.start()
	return m
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

	firstChunk := newReaderPromise()

	firstReqResultCh := make(chan firstReqResult)
	m.queue.submitLow(func(buf []byte) {
		defer close(firstReqResultCh)
		firstChunkResp, err := m.DoRequest(ctx, 0, m.chunkSize()-1, url)
		if err != nil {
			firstReqResultCh <- firstReqResult{err: err}
			return
		}

		defer firstChunkResp.Body.Close()

		trueURL := firstChunkResp.Request.URL.String()
		if trueURL != url {
			logger.Info().Str("url", url).Str("redirect_url", trueURL).Msg("Redirect")
		}

		fileSize, err := m.getFileSizeFromContentRange(firstChunkResp.Header.Get("Content-Range"))
		if err != nil {
			firstReqResultCh <- firstReqResult{err: err}
			return
		}
		firstReqResultCh <- firstReqResult{fileSize: fileSize, trueURL: trueURL}

		contentLength := firstChunkResp.ContentLength
		n, err := io.ReadFull(firstChunkResp.Body, buf[0:contentLength])
		if err == io.ErrUnexpectedEOF {
			_, err = resumeDownload(firstChunkResp.Request, buf[n:], m.Client, int64(n))
		}
		firstChunk.Deliver(buf[0:n], err)
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
		return firstChunk, fileSize, nil
	}

	remainingBytes := fileSize - m.chunkSize()
	// integer divide rounding up
	numChunks := int((remainingBytes-1)/m.chunkSize() + 1)

	chunks := make([]io.Reader, numChunks+1)
	chunks[0] = firstChunk

	startOffset := m.chunkSize()

	logger.Debug().Str("url", url).
		Int64("size", fileSize).
		Int("connections", numChunks).
		Int64("chunkSize", m.chunkSize()).
		Msg("Downloading")

	for i := 0; i < numChunks; i++ {
		chunk := newReaderPromise()
		chunks[i+1] = chunk
	}
	go func(chunks []io.Reader) {
		for i, reader := range chunks {
			chunk := reader.(*readerPromise)
			m.queue.submitHigh(func(buf []byte) {
				start := startOffset + m.chunkSize()*int64(i)
				end := start + m.chunkSize() - 1

				if i == numChunks-1 {
					end = fileSize - 1
				}
				logger.Debug().Str("url", url).
					Int64("size", fileSize).
					Int("chunk", i).
					Msg("Downloading chunk")

				resp, err := m.DoRequest(ctx, start, end, trueURL)
				if err != nil {
					chunk.Deliver(nil, err)
					return
				}
				defer resp.Body.Close()

				contentLength := resp.ContentLength
				n, err := io.ReadFull(resp.Body, buf[0:contentLength])
				if err == io.ErrUnexpectedEOF {
					_, err = resumeDownload(resp.Request, buf[n:], m.Client, int64(n))
				}
				chunk.Deliver(buf[0:n], err)
			})
		}
	}(chunks[1:])

	return io.MultiReader(chunks...), fileSize, nil
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

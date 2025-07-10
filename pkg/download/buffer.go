package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/logging"
)

type BufferMode struct {
	Client client.HTTPClient
	Options

	queue      *priorityWorkQueue
	redirected bool
}

func GetBufferMode(opts Options) *BufferMode {
	client := client.NewHTTPClient(opts.Client)
	m := &BufferMode{
		Client:     client,
		Options:    opts,
		redirected: false,
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

func (m *BufferMode) getFileSizeFromResponse(resp *http.Response) (int64, error) {
	// If the response is a 200 OK, we need to parse the file size assuming the whole
	// file was returned. If it isn't, we will assume this was a 206 Partial Content
	// and parse the file size from the content range header. We wouldn't be in this
	// function if the response was not between 200 and 300, so this feels like a
	// reasonable assumption. If we get a content range header though, we should
	// always use that
	if resp.StatusCode == http.StatusOK && resp.Header.Get("Content-Range") == "" {
		return m.getFileSizeFromContentLength(resp.Header.Get("Content-Length"))
	}
	return m.getFileSizeFromContentRange(resp.Header.Get("Content-Range"))
}

func (m *BufferMode) getFileSizeFromContentLength(contentLength string) (int64, error) {
	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return 0, err
	}

	return size, nil
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

		if m.CacheHosts != nil {
			url = m.rewriteUrlForCache(url)
		}

		firstChunkResp, err := m.DoRequest(ctx, 0, m.chunkSize()-1, url)
		if err != nil {
			firstReqResultCh <- firstReqResult{err: err}
			return
		}

		defer firstChunkResp.Body.Close()

		trueURL := firstChunkResp.Request.URL.String()
		if trueURL != url {
			logger.Info().Str("url", url).Str("redirect_url", trueURL).Msg("Redirect")
			m.redirected = true
		}

		fileSize, err := m.getFileSizeFromResponse(firstChunkResp)
		if err != nil {
			firstReqResultCh <- firstReqResult{err: err}
			return
		}
		firstReqResultCh <- firstReqResult{fileSize: fileSize, trueURL: trueURL}

		contentLength := firstChunkResp.ContentLength
		n, err := io.ReadFull(firstChunkResp.Body, buf[0:contentLength])
		if err == io.ErrUnexpectedEOF {
			logger.Warn().
				Int("connection_interrupted_at_byte", n).
				Msg("Resuming Chunk Download")
			n, err = resumeDownload(firstChunkResp.Request, buf[n:contentLength], m.Client, int64(n))
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
					logger.Warn().
						Int("connection_interrupted_at_byte", n).
						Msg("Resuming Chunk Download")
					n, err = resumeDownload(resp.Request, buf[n:contentLength], m.Client, int64(n))
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
	proxyAuthHeader := viper.GetString(config.OptProxyAuthHeader)
	if proxyAuthHeader != "" && !m.redirected {
		req.Header.Set("Authorization", proxyAuthHeader)
	}
	resp, err := m.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request for %s: %w", req.URL.String(), err)
	}
	if resp.StatusCode == 0 || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w %s: %s", ErrUnexpectedHTTPStatus, req.URL.String(), resp.Status)
	}

	return resp, nil
}

func (m *BufferMode) rewriteUrlForCache(urlString string) string {
	logger := logging.GetLogger()
	parsed, err := url.Parse(urlString)
	if m.CacheHosts == nil || len(m.CacheHosts) != 1 {
		logger.Error().
			Str("url", urlString).
			Bool("enabled", false).
			Str("disabled_reason", fmt.Sprintf("expected exactly 1 cache host, received %d", len(m.CacheHosts))).
			Msg("Cache URL Rewrite")
		return urlString
	}
	if strings.HasPrefix(urlString, m.CacheHosts[0]) {
		logger.Info().
			Str("url", urlString).
			Str("target_url", urlString).
			Bool("enabled", true).
			Msg("Cache URL already rewritten")
		return urlString
	}
	if err != nil {
		logger.Error().
			Err(err).
			Str("url", urlString).
			Bool("enabled", false).
			Str("disabled_reason", "failed to parse URL").
			Msg("Cache URL Rewrite")
		return urlString
	}
	if m.ForceCachePrefixRewrite {
		// Forcefully rewrite the URL prefix
		return m.rewritePrefix(m.CacheHosts[0], urlString, parsed, logger)
	} else {
		if prefixes, ok := m.CacheableURIPrefixes[parsed.Host]; ok {
			for _, pfx := range prefixes {
				if pfx.Path == "/" || strings.HasPrefix(parsed.Path, pfx.Path) {
					// Found a matching prefix, rewrite the URL prefix
					return m.rewritePrefix(m.CacheHosts[0], urlString, parsed, logger)
				}
			}
		}
	}

	// If we got here, we weren't forcefully rewriting the cache prefix and we didn't
	// find any matching prefixes, so we just return the original URL
	logger.Debug().
		Str("url", urlString).
		Bool("enabled", false).
		Str("disabled_reason", "no matching prefix").
		Str("disabled_reason", "failed to join host URL to path").
		Msg("Cache URL Rewrite")
	return urlString
}

func (m *BufferMode) rewritePrefix(cacheHost, urlString string, parsed *url.URL, logger zerolog.Logger) string {
	newUrl := cacheHost
	var err error
	if m.CacheUsePathProxy {
		newUrl, err = url.JoinPath(newUrl, parsed.Host)
		if err != nil {
			logger.Error().
				Err(err).
				Str("url", urlString).
				Bool("enabled", false).
				Str("disabled_reason", "failed to join cache URL to host").
				Msg("Cache URL Rewrite")
			return urlString
		}
		logger.Debug().
			Bool("path_based_proxy", true).
			Str("host_prefix", parsed.Host).
			Str("intermediate_target_url", newUrl).
			Str("url", urlString).
			Msg("Cache URL Rewrite")
	}
	newUrl, err = url.JoinPath(newUrl, parsed.Path)
	if err != nil {
		logger.Error().
			Err(err).
			Str("url", urlString).
			Bool("enabled", false).
			Str("disabled_reason", "failed to join host URL to path").
			Msg("Cache URL Rewrite")
		return urlString
	}
	if parsed.RawQuery != "" {
		newUrl = fmt.Sprintf("%s?%s", newUrl, parsed.RawQuery)
	}
	logger.Info().
		Str("url", urlString).
		Str("target_url", newUrl).
		Bool("enabled", true).
		Msg("Cache URL Rewrite")
	return newUrl
}

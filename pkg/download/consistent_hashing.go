package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/consistent"
	"github.com/replicate/pget/pkg/logging"
)

type ConsistentHashingMode struct {
	Client client.HTTPClient
	Options
	// TODO: allow this to be configured and not just "BufferMode"
	FallbackStrategy Strategy

	queue *priorityWorkQueue
}

type CacheKey struct {
	URL   *url.URL `hash:"string"`
	Slice int64
}

func GetConsistentHashingMode(opts Options) (*ConsistentHashingMode, error) {
	if opts.SliceSize == 0 {
		return nil, fmt.Errorf("must specify slice size in consistent hashing mode")
	}
	client := client.NewHTTPClient(opts.Client)

	fallbackStrategy := &BufferMode{
		Client:  client,
		Options: opts,
	}

	m := &ConsistentHashingMode{
		Client:           client,
		Options:          opts,
		FallbackStrategy: fallbackStrategy,
	}
	m.queue = newWorkQueue(opts.maxConcurrency(), m.chunkSize())
	m.queue.start()
	fallbackStrategy.queue = m.queue
	return m, nil
}

func (m *ConsistentHashingMode) chunkSize() int64 {
	chunkSize := m.ChunkSize
	if chunkSize == 0 {
		chunkSize = defaultChunkSize
	}
	if chunkSize > m.SliceSize {
		chunkSize = m.SliceSize
	}
	return chunkSize
}

func (m *ConsistentHashingMode) getFileSizeFromContentRange(contentRange string) (int64, error) {
	groups := contentRangeRegexp.FindStringSubmatch(contentRange)
	if groups == nil {
		return -1, fmt.Errorf("couldn't parse Content-Range: %s", contentRange)
	}
	return strconv.ParseInt(groups[1], 10, 64)
}

func (m *ConsistentHashingMode) Fetch(ctx context.Context, urlString string) (io.Reader, int64, error) {
	logger := logging.GetLogger()

	parsed, err := url.Parse(urlString)
	if err != nil {
		return nil, -1, err
	}
	shouldContinue := false
	if prefixes, ok := m.CacheableURIPrefixes[parsed.Host]; ok {
		for _, pfx := range prefixes {
			if pfx.Path == "/" || strings.HasPrefix(parsed.Path, pfx.Path) {
				shouldContinue = true
				break
			}
		}
	}
	// Use our fallback mode if we're not downloading from a consistent-hashing enabled domain
	if !shouldContinue {
		logger.Debug().
			Str("url", urlString).
			Str("reason", fmt.Sprintf("consistent hashing not enabled for %s", parsed.Host)).
			Msg("fallback strategy")
		return m.FallbackStrategy.Fetch(ctx, urlString)
	}

	firstChunk := newReaderPromise()
	firstReqResultCh := make(chan firstReqResult)
	m.queue.submitLow(func(buf []byte) {
		defer close(firstReqResultCh)
		firstChunkResp, err := m.DoRequest(ctx, 0, m.chunkSize()-1, urlString)
		if err != nil {
			firstReqResultCh <- firstReqResult{err: err}
			return
		}
		defer firstChunkResp.Body.Close()

		fileSize, err := m.getFileSizeFromContentRange(firstChunkResp.Header.Get("Content-Range"))
		if err != nil {
			firstReqResultCh <- firstReqResult{err: err}
			return
		}
		firstReqResultCh <- firstReqResult{fileSize: fileSize}

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
		panic("logic error in ConsistentHashingMode: first request didn't return any output")
	}
	if firstReqResult.err != nil {
		// In the case that an error indicating an issue with the cache server, networking, etc is returned,
		// this will use the fallback strategy. This is a case where the whole file will use the fallback
		// strategy.
		if errors.Is(firstReqResult.err, client.ErrStrategyFallback) {
			// TODO(morgan): we should indicate the fallback strategy we're using in the logs
			logger.Info().
				Str("url", urlString).
				Str("type", "file").
				Err(err).
				Msg("consistent hash fallback")
			return m.FallbackStrategy.Fetch(ctx, urlString)
		}
		return nil, -1, firstReqResult.err
	}
	fileSize := firstReqResult.fileSize

	if fileSize <= m.chunkSize() {
		// we only need a single chunk: just download it and finish
		return firstChunk, fileSize, nil
	}

	totalSlices := fileSize / m.SliceSize
	if fileSize%m.SliceSize != 0 {
		totalSlices++
	}

	readers := make([]io.Reader, 0)
	slices := make([][]*readerPromise, totalSlices)
	logger.Debug().Str("url", urlString).
		Int64("size", fileSize).
		Int("concurrency", m.maxConcurrency()).
		Msg("Downloading")

	for slice := 0; slice < int(totalSlices); slice++ {
		sliceSize := m.SliceSize
		if slice == int(totalSlices)-1 {
			sliceSize = (fileSize-1)%m.SliceSize + 1
		}
		// integer divide rounding up
		numChunks := int(((sliceSize - 1) / m.chunkSize()) + 1)
		chunks := make([]*readerPromise, numChunks)
		for i := 0; i < numChunks; i++ {
			var chunk *readerPromise
			if slice == 0 && i == 0 {
				chunk = firstChunk
			} else {
				chunk = newReaderPromise()
			}
			chunks[i] = chunk
			readers = append(readers, chunk)
		}
		slices[slice] = chunks
	}
	go m.downloadRemainingChunks(ctx, urlString, slices)
	return io.MultiReader(readers...), fileSize, nil
}

func (m *ConsistentHashingMode) downloadRemainingChunks(ctx context.Context, urlString string, slices [][]*readerPromise) {
	logger := logging.GetLogger()
	for slice, sliceChunks := range slices {
		sliceStart := m.SliceSize * int64(slice)
		sliceEnd := m.SliceSize*int64(slice+1) - 1
		for i, chunk := range sliceChunks {
			if slice == 0 && i == 0 {
				// this is the first chunk, already handled above
				continue
			}
			m.queue.submitHigh(func(buf []byte) {
				chunkStart := sliceStart + int64(i)*m.chunkSize()
				chunkEnd := chunkStart + m.chunkSize() - 1
				if chunkEnd > sliceEnd {
					chunkEnd = sliceEnd
				}

				logger.Debug().Int64("start", chunkStart).Int64("end", chunkEnd).Msg("starting request")
				resp, err := m.DoRequest(ctx, chunkStart, chunkEnd, urlString)
				if err != nil {
					// in the case that an error indicating an issue with the cache server, networking, etc is returned,
					// this will use the fallback strategy. This is a case where the whole file will perform the fall-back
					// for the specified chunk instead of the whole file.
					if errors.Is(err, client.ErrStrategyFallback) {
						// TODO(morgan): we should indicate the fallback strategy we're using in the logs
						logger.Info().
							Str("url", urlString).
							Str("type", "chunk").
							Err(err).
							Msg("consistent hash fallback")
						resp, err = m.FallbackStrategy.DoRequest(ctx, chunkStart, chunkEnd, urlString)
					}
					if err != nil {
						chunk.Deliver(nil, err)
						return
					}
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
	}
}

func (m *ConsistentHashingMode) DoRequest(ctx context.Context, start, end int64, urlString string) (*http.Response, error) {
	chContext := context.WithValue(ctx, config.ConsistentHashingStrategyKey, true)
	req, err := http.NewRequestWithContext(chContext, "GET", urlString, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s: %w", req.URL.String(), err)
	}
	resp, cachePodIndex, err := m.doRequestToCacheHost(req, urlString, start, end)
	if err != nil {
		if errors.Is(err, client.ErrStrategyFallback) {
			origErr := err
			req, err := http.NewRequestWithContext(chContext, "GET", urlString, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to download %s: %w", req.URL.String(), err)
			}
			resp, _, err = m.doRequestToCacheHost(req, urlString, start, end, cachePodIndex)
			if err != nil {
				// return origErr so that we can use our regular fallback strategy
				return nil, origErr
			}
		} else {
			return nil, fmt.Errorf("error executing request for %s: %w", req.URL.String(), err)
		}
	}
	if resp.StatusCode == 0 || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w %s: %s", ErrUnexpectedHTTPStatus, req.URL.String(), resp.Status)
	}

	return resp, nil
}

func (m *ConsistentHashingMode) doRequestToCacheHost(req *http.Request, urlString string, start int64, end int64, previousPodIndexes ...int) (*http.Response, int, error) {
	logger := logging.GetLogger()
	cachePodIndex, err := m.rewriteRequestToCacheHost(req, start, end, previousPodIndexes...)
	if err != nil {
		return nil, cachePodIndex, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	logger.Debug().Str("url", urlString).Str("munged_url", req.URL.String()).Str("host", req.Host).Int64("start", start).Int64("end", end).Msg("request")

	resp, err := m.Client.Do(req)
	return resp, cachePodIndex, err
}

func (m *ConsistentHashingMode) rewriteRequestToCacheHost(req *http.Request, start int64, end int64, previousPodIndexes ...int) (int, error) {
	logger := logging.GetLogger()
	if start/m.SliceSize != end/m.SliceSize {
		return 0, fmt.Errorf("Internal error: can't make a range request across a slice boundary: %d-%d straddles a slice boundary (slice size is %d)", start, end, m.SliceSize)
	}
	slice := start / m.SliceSize

	key := CacheKey{URL: req.URL, Slice: slice}

	cachePodIndex, err := consistent.HashBucket(key, len(m.CacheHosts), previousPodIndexes...)
	if err != nil {
		return -1, err
	}
	if m.CacheUsePathProxy {
		// prepend the hostname to the start of the path. The consistent-hash nodes will use this to determine the proxy
		newPath, err := url.JoinPath(strings.ToLower(req.URL.Host), req.URL.Path)
		if err != nil {
			return -1, err
		}
		// Ensure wr have a leading slash, things get weird (especially in testing) if we do not.
		req.URL.Path = fmt.Sprintf("/%s", newPath)
	}
	cacheHost := m.CacheHosts[cachePodIndex]
	if cacheHost == "" {
		// this can happen if an SRV record is missing due to a not-ready pod
		logger.Debug().
			Str("cache_key", fmt.Sprintf("%+v", key)).
			Int64("start", start).
			Int64("end", end).
			Int64("slice_size", m.SliceSize).
			Int("bucket", cachePodIndex).
			Ints("previous_pod_indexes", previousPodIndexes).
			Msg("cache host for bucket not ready, falling back")
		return cachePodIndex, client.ErrStrategyFallback
	}
	logger.Debug().
		Str("cache_key", fmt.Sprintf("%+v", key)).
		Int64("start", start).
		Int64("end", end).
		Int64("slice_size", m.SliceSize).
		Int("bucket", cachePodIndex).
		Ints("previous_pod_indexes", previousPodIndexes).
		Msg("consistent hashing")
	req.URL.Scheme = "http"
	req.URL.Host = cacheHost

	return cachePodIndex, nil
}

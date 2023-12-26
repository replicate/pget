package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/consistent"
	"github.com/replicate/pget/pkg/logging"
)

type ConsistentHashingMode struct {
	Client *client.HTTPClient
	Options
	// TODO: allow this to be configured and not just "BufferMode"
	FallbackStrategy Strategy

	// we use this errgroup as a semaphore (via sem.SetLimit())
	sem   *errgroup.Group
	queue *workQueue
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
	sem := new(errgroup.Group)
	sem.SetLimit(opts.maxConcurrency())
	queue := newWorkQueue(opts.maxConcurrency())
	queue.start()

	fallbackStrategy := &BufferMode{
		Client:  client,
		Options: opts,
		sem:     sem,
		queue:   queue,
	}

	return &ConsistentHashingMode{
		Client:           client,
		Options:          opts,
		FallbackStrategy: fallbackStrategy,
		sem:              sem,
		queue:            queue,
	}, nil
}

func (m *ConsistentHashingMode) minChunkSize() int64 {
	minChunkSize := m.MinChunkSize
	if minChunkSize == 0 {
		minChunkSize = defaultMinChunkSize
	}
	if minChunkSize > m.SliceSize {
		minChunkSize = m.SliceSize
	}
	return minChunkSize
}

func (m *ConsistentHashingMode) getFileSizeFromContentRange(contentRange string) (int64, error) {
	groups := contentRangeRegexp.FindStringSubmatch(contentRange)
	if groups == nil {
		return -1, fmt.Errorf("couldn't parse Content-Range: %s", contentRange)
	}
	return strconv.ParseInt(groups[1], 10, 64)
}

var _ http.Handler = &ConsistentHashingMode{}

func (m *ConsistentHashingMode) Fetch(ctx context.Context, urlString string) (io.Reader, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlString, nil)
	if err != nil {
		return nil, 0, err
	}
	return m.fetch(req)
}

func (m *ConsistentHashingMode) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	// if we want to forward req, we must blank out req.RequestURI
	req.RequestURI = ""
	// client requests don't have scheme or host set on the request. We need to
	// restore it for hash consistency
	req.URL.Scheme = "https"
	req.URL.Host = req.Host
	reader, size, err := m.fetch(req)
	if err != nil {
		var httpErr HttpStatusError
		if errors.As(err, &httpErr) {
			resp.WriteHeader(httpErr.StatusCode)
		} else {
			resp.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	// TODO: http.StatusPartialContent and Content-Range if it was a range request
	resp.Header().Set("Content-Length", fmt.Sprint(size))
	resp.WriteHeader(http.StatusOK)
	// we ignore errors as it's too late to change status code
	_, _ = io.Copy(resp, reader)
}

func (m *ConsistentHashingMode) fetch(req *http.Request) (io.Reader, int64, error) {
	logger := logging.GetLogger()

	shouldContinue := false
	for _, host := range m.DomainsToCache {
		if host == req.Host {
			shouldContinue = true
			break
		}
	}
	// Use our fallback mode if we're not downloading from a consistent-hashing enabled domain
	if !shouldContinue {
		logger.Debug().
			Str("url", req.URL.String()).
			Str("reason", fmt.Sprintf("consistent hashing not enabled for %s", req.Host)).
			Msg("fallback strategy")
		return m.FallbackStrategy.Fetch(req.Context(), req.URL.String())
	}

	br := newBufferedReader(m.minChunkSize())
	firstReqResultCh := make(chan firstReqResult)
	m.queue.submit(func() {
		m.sem.Go(func() error {
			defer close(firstReqResultCh)
			defer br.done()
			// TODO: respect Range header in the original request
			firstChunkResp, err := m.DoRequest(req, 0, m.minChunkSize()-1)
			if err != nil {
				firstReqResultCh <- firstReqResult{err: err}
				return err
			}
			defer firstChunkResp.Body.Close()

			fileSize, err := m.getFileSizeFromContentRange(firstChunkResp.Header.Get("Content-Range"))
			if err != nil {
				firstReqResultCh <- firstReqResult{err: err}
				return err
			}
			firstReqResultCh <- firstReqResult{fileSize: fileSize}

			return br.downloadBody(firstChunkResp)
		})
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
				Str("url", req.URL.String()).
				Str("type", "file").
				Err(firstReqResult.err).
				Msg("consistent hash fallback")
			return m.FallbackStrategy.Fetch(req.Context(), req.URL.String())
		}
		return nil, -1, firstReqResult.err
	}
	fileSize := firstReqResult.fileSize

	if fileSize <= m.minChunkSize() {
		// we only need a single chunk: just download it and finish
		return br, fileSize, nil
	}

	totalSlices := fileSize / m.SliceSize
	if fileSize%m.SliceSize != 0 {
		totalSlices++
	}

	// we subtract one because we've already got firstChunkResp in flight
	concurrency := m.maxConcurrency() - 1
	if concurrency <= int(totalSlices) {
		// special case: we should just download each slice in full and rely on the semaphore to manage concurrency
		concurrency = int(totalSlices)
	}

	chunksPerSlice := EqualSplit(int64(concurrency), totalSlices)
	if m.minChunkSize() == m.SliceSize {
		// firstChunkResp will download the whole first chunk in full;
		// we set slice 0 to a special value of 0 so we skip it later
		chunksPerSlice = append([]int64{0}, EqualSplit(int64(concurrency), totalSlices-1)...)
	}

	readersCh := make(chan io.Reader, m.maxConcurrency()+1)
	readersCh <- br

	logger.Debug().Str("url", req.URL.String()).
		Int64("size", fileSize).
		Int("concurrency", m.maxConcurrency()).
		Ints64("chunks_per_slice", chunksPerSlice).
		Msg("Downloading")

	m.queue.submit(func() {
		defer close(readersCh)
		for slice, numChunks := range chunksPerSlice {
			if numChunks == 0 {
				// this happens if we've already downloaded the whole first slice
				continue
			}
			startFrom := m.SliceSize * int64(slice)
			sliceSize := m.SliceSize
			if slice == 0 {
				startFrom = m.minChunkSize()
				sliceSize = sliceSize - m.minChunkSize()
			}
			if slice == int(totalSlices)-1 {
				sliceSize = (fileSize-1)%m.SliceSize + 1
			}
			if sliceSize/numChunks < m.minChunkSize() {
				// reset numChunks to respect minChunkSize
				numChunks = sliceSize / m.minChunkSize()
				// although we must always have at least one chunk
				if numChunks == 0 {
					numChunks = 1
				}
			}
			chunkSizes := EqualSplit(sliceSize, numChunks)
			for _, chunkSize := range chunkSizes {
				// startFrom changes each time round the loop
				// we create chunkStart to be a stable variable for the goroutine to capture
				chunkStart := startFrom
				chunkEnd := startFrom + chunkSize - 1

				br := newBufferedReader(m.minChunkSize())
				readersCh <- br
				m.sem.Go(func() error {
					defer br.done()
					logger.Debug().Int64("start", chunkStart).Int64("end", chunkEnd).Msg("starting request")
					resp, err := m.DoRequest(req, chunkStart, chunkEnd)
					if err != nil {
						// in the case that an error indicating an issue with the cache server, networking, etc is returned,
						// this will use the fallback strategy. This is a case where the whole file will perform the fall-back
						// for the specified chunk instead of the whole file.
						if errors.Is(err, client.ErrStrategyFallback) {
							// TODO(morgan): we should indicate the fallback strategy we're using in the logs
							logger.Info().
								Str("url", req.URL.String()).
								Str("type", "chunk").
								Err(err).
								Msg("consistent hash fallback")
							resp, err = m.FallbackStrategy.DoRequest(req, chunkStart, chunkEnd)
						}
						if err != nil {
							return err
						}
					}
					defer resp.Body.Close()
					return br.downloadBody(resp)
				})

				startFrom = startFrom + chunkSize
			}
		}
	})

	return newChanMultiReader(readersCh), fileSize, nil
}

func (m *ConsistentHashingMode) DoRequest(origReq *http.Request, start, end int64) (*http.Response, error) {
	logger := logging.GetLogger()
	chContext := context.WithValue(origReq.Context(), config.ConsistentHashingStrategyKey, true)
	req := origReq.Clone(chContext)
	cachePodIndex, err := m.rewriteRequestToCacheHost(req, start, end)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	logger.Debug().Str("url", req.URL.String()).Str("munged_url", req.URL.String()).Str("host", req.Host).Int64("start", start).Int64("end", end).Msg("request")

	resp, err := m.Client.Do(req)
	if err != nil {
		if errors.Is(err, client.ErrStrategyFallback) {
			origErr := err
			req = origReq.Clone(chContext)
			_, err = m.rewriteRequestToCacheHost(req, start, end, cachePodIndex)
			if err != nil {
				// return origErr so that we can use our regular fallback strategy
				return nil, origErr
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
			logger.Debug().Str("url", origReq.URL.String()).Str("munged_url", req.URL.String()).Str("host", req.Host).Int64("start", start).Int64("end", end).Msg("retry request")

			resp, err = m.Client.Do(req)
			if err != nil {
				// return origErr so that we can use our regular fallback strategy
				return nil, origErr
			}
		} else {
			return nil, fmt.Errorf("error executing request for %s: %w", req.URL.String(), err)
		}
	}
	if resp.StatusCode == 0 || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode >= 400 {
			return nil, HttpStatusError{StatusCode: resp.StatusCode}
		}

		return nil, fmt.Errorf("%w %s", ErrUnexpectedHTTPStatus(resp.StatusCode), req.URL.String())
	}

	return resp, nil
}

func (m *ConsistentHashingMode) rewriteRequestToCacheHost(req *http.Request, start int64, end int64, previousPodIndexes ...int) (int, error) {
	logger := logging.GetLogger()
	if start/m.SliceSize != end/m.SliceSize {
		return 0, fmt.Errorf("can't make a range request across a slice boundary: %d-%d straddles a slice boundary (slice size is %d)", start, end, m.SliceSize)
	}
	slice := start / m.SliceSize

	key := CacheKey{URL: req.URL, Slice: slice}

	cachePodIndex, err := consistent.HashBucket(key, len(m.CacheHosts), previousPodIndexes...)
	if err != nil {
		return -1, err
	}
	cacheHost := m.CacheHosts[cachePodIndex]
	logger.Debug().Str("cache_key", fmt.Sprintf("%+v", key)).Int64("start", start).Int64("end", end).Int64("slice_size", m.SliceSize).Int("bucket", cachePodIndex).Msg("consistent hashing")
	if cacheHost != "" {
		req.URL.Scheme = "http"
		req.URL.Host = cacheHost
	}
	return cachePodIndex, nil
}

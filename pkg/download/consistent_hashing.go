package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	jump "github.com/dgryski/go-jump"
	"github.com/mitchellh/hashstructure/v2"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/config"
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

func (m *ConsistentHashingMode) Fetch(ctx context.Context, urlString string) (io.Reader, int64, error) {
	logger := logging.GetLogger()

	parsed, err := url.Parse(urlString)
	if err != nil {
		return nil, -1, err
	}
	shouldContinue := false
	for _, host := range m.DomainsToCache {
		if host == parsed.Host {
			shouldContinue = true
			break
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

	br := newBufferedReader(m.minChunkSize())
	firstReqResultCh := make(chan firstReqResult)
	m.queue.submit(func() {
		m.sem.Go(func() error {
			defer close(firstReqResultCh)
			defer br.done()
			firstChunkResp, err := m.DoRequest(ctx, 0, m.minChunkSize()-1, urlString)
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
			return m.FallbackStrategy.Fetch(ctx, urlString)
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

	logger.Debug().Str("url", urlString).
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
					resp, err := m.DoRequest(ctx, chunkStart, chunkEnd, urlString)
					if err != nil {
						// in the case that an error indicating an issue with the cache server, networking, etc is returned,
						// this will use the fallback strategy. This is a case where the whole file will perform the fall-back
						// for the specified chunk instead of the whole file.
						if errors.Is(err, client.ErrStrategyFallback) {
							resp, err = m.FallbackStrategy.DoRequest(ctx, chunkStart, chunkEnd, urlString)
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

func (m *ConsistentHashingMode) DoRequest(ctx context.Context, start, end int64, urlString string) (*http.Response, error) {
	logger := logging.GetLogger()
	chContext := context.WithValue(ctx, config.ConsistentHashingStrategyKey, true)
	req, err := http.NewRequestWithContext(chContext, "GET", urlString, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s: %w", req.URL.String(), err)
	}
	err = m.consistentHashIfNeeded(req, start, end)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	logger.Debug().Str("url", urlString).Str("munged_url", req.URL.String()).Str("host", req.Host).Int64("start", start).Int64("end", end).Msg("request")

	resp, err := m.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request for %s: %w", req.URL.String(), err)
	}
	if resp.StatusCode == 0 || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w %s: %s", ErrUnexpectedHTTPStatus, req.URL.String(), resp.Status)
	}

	return resp, nil
}

func (m *ConsistentHashingMode) consistentHashIfNeeded(req *http.Request, start int64, end int64) error {
	logger := logging.GetLogger()
	for _, host := range m.DomainsToCache {
		if host == req.URL.Host {
			if start/m.SliceSize != end/m.SliceSize {
				return fmt.Errorf("can't make a range request across a slice boundary: %d-%d straddles a slice boundary (slice size is %d)", start, end, m.SliceSize)
			}
			slice := start / m.SliceSize

			key := CacheKey{URL: req.URL, Slice: slice}
			// we set IgnoreZeroValue so that we can add fields to the hash key
			// later without breaking things.
			// note that it's not safe to share a HashOptions so we create a fresh one each time.
			hashopts := &hashstructure.HashOptions{IgnoreZeroValue: true}
			hash, err := hashstructure.Hash(key, hashstructure.FormatV2, hashopts)
			if err != nil {
				return fmt.Errorf("error calculating hash of key")
			}

			logger.Debug().Uint64("hash_sum", hash).Int("len_cache_hosts", len(m.CacheHosts)).Msg("consistent hashing")

			// jump is an implementation of Google's Jump Consistent Hash.
			//
			// See http://arxiv.org/abs/1406.2294 for details.
			cachePodIndex := int(jump.Hash(hash, len(m.CacheHosts)))
			cacheHost := m.CacheHosts[cachePodIndex]
			logger.Debug().Str("cache_key", fmt.Sprintf("%+v", key)).Int64("start", start).Int64("end", end).Int64("slice_size", m.SliceSize).Int("bucket", cachePodIndex).Msg("consistent hashing")
			if cacheHost != "" {
				req.URL.Scheme = "http"
				req.URL.Host = cacheHost
			}
			return nil
		}
	}
	return nil
}

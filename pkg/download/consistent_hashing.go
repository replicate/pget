package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strconv"

	jump "github.com/dgryski/go-jump"
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
}

func GetConsistentHashingMode(opts Options) (Strategy, error) {
	if opts.SliceSize == 0 {
		return nil, fmt.Errorf("must specify slice size in consistent hashing mode")
	}
	if opts.Semaphore != nil && opts.MaxConcurrency == 0 {
		return nil, fmt.Errorf("if you provide a semaphore you must specify MaxConcurrency")
	}
	client := client.NewHTTPClient(opts.Client)
	fallbackStrategy := GetBufferMode(opts)
	fallbackStrategy.Client = client

	return &ConsistentHashingMode{
		Client:           client,
		Options:          opts,
		FallbackStrategy: fallbackStrategy,
	}, nil
}

func (m *ConsistentHashingMode) maxConcurrency() int {
	maxChunks := m.MaxConcurrency
	if maxChunks == 0 {
		return runtime.NumCPU() * 4
	}
	return maxChunks
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

	firstChunkResp, err := m.DoRequest(ctx, 0, m.minChunkSize()-1, urlString)
	if err != nil {
		// In the case that an error indicating an issue with the cache server, networking, etc is returned,
		// this will use the fallback strategy. This is a case where the whole file will use the fallback
		// strategy.
		if errors.Is(err, client.ErrStrategyFallback) {
			return m.FallbackStrategy.Fetch(ctx, urlString)
		}
		return nil, -1, err
	}

	fileSize, err := m.getFileSizeFromContentRange(firstChunkResp.Header.Get("Content-Range"))
	if err != nil {
		firstChunkResp.Body.Close()
		return nil, -1, err
	}

	data := make([]byte, fileSize)
	if fileSize <= m.minChunkSize() {
		// we only need a single chunk: just download it and finish
		err = m.downloadChunk(firstChunkResp, data)
		if err != nil {
			return nil, -1, err
		}
		// TODO: rather than eagerly downloading here, we could return
		// an io.ReadCloser that downloads the file and releases the
		// semaphore when closed
		return bytes.NewBuffer(data), fileSize, nil
	}

	totalSlices := fileSize / m.SliceSize
	if fileSize%m.SliceSize != 0 {
		totalSlices++
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		return m.downloadChunk(firstChunkResp, data[0:m.minChunkSize()])
	})

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

	logger.Debug().Str("url", urlString).
		Int64("size", fileSize).
		Int("concurrency", m.maxConcurrency()).
		Ints64("chunks_per_slice", chunksPerSlice).
		Msg("Downloading")

	for slice, numChunks := range chunksPerSlice {
		if numChunks == 0 {
			// this happens if we've already downloaded the whole first slice
			continue
		}
		startFrom := m.SliceSize * int64(slice)
		sliceSize := m.SliceSize
		if slice == 0 {
			startFrom = firstChunkResp.ContentLength
			sliceSize = sliceSize - firstChunkResp.ContentLength
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

			dataSlice := data[chunkStart : chunkEnd+1]
			errGroup.Go(func() error {
				logger.Debug().Int64("start", chunkStart).Int64("end", chunkEnd).Msg("starting request")
				resp, err := m.DoRequest(ctx, chunkStart, chunkEnd, urlString)
				if err != nil {
					// In the case that an error indicating an issue with the cache server, networking, etc is returned,
					// this will use the fallback strategy. This is a case where the whole file will perform the fall-back
					// for the specified chunk instead of the whole file.
					if errors.Is(err, client.ErrStrategyFallback) {
						resp, err = m.FallbackStrategy.DoRequest(ctx, chunkStart, chunkEnd, urlString)
					}
					if err != nil {
						return err
					}
				}

				return m.downloadChunk(resp, dataSlice)
			})

			startFrom = startFrom + chunkSize
		}
	}

	if err := errGroup.Wait(); err != nil {
		return nil, -1, err // return the first error we encounter
	}

	buffer := bytes.NewBuffer(data)
	return buffer, fileSize, nil
}

func (m *ConsistentHashingMode) DoRequest(ctx context.Context, start, end int64, urlString string) (*http.Response, error) {
	logger := logging.GetLogger()
	if m.Semaphore != nil {
		err := m.Semaphore.Acquire(ctx, 1)
		if err != nil {
			return nil, err
		}
	}
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

			key := fmt.Sprintf("%s#%d", req.URL, slice)
			hasher := fnv.New64a()
			if _, err := hasher.Write([]byte(key)); err != nil {
				return fmt.Errorf("error calculating hash of key")
			}
			// jump is an implementation of Google's Jump Consistent Hash.
			//
			// See http://arxiv.org/abs/1406.2294 for details.
			logger.Debug().Uint64("hash_sum", hasher.Sum64()).Int("len_cache_hosts", len(m.CacheHosts)).Msg("consistent hashing")
			cachePodIndex := int(jump.Hash(hasher.Sum64(), len(m.CacheHosts)))
			cacheHost := m.CacheHosts[cachePodIndex]
			logger.Debug().Str("cache_key", key).Int64("start", start).Int64("end", end).Int64("slice_size", m.SliceSize).Int("bucket", cachePodIndex).Msg("consistent hashing")
			if cacheHost != "" {
				req.URL.Scheme = "http"
				req.URL.Host = cacheHost
			}
			return nil
		}
	}
	return nil
}

func (m *ConsistentHashingMode) downloadChunk(resp *http.Response, dataSlice []byte) error {
	logger := logging.GetLogger()
	defer resp.Body.Close()
	if m.Semaphore != nil {
		defer m.Semaphore.Release(1)
	}
	expectedBytes := len(dataSlice)
	n, err := io.ReadFull(resp.Body, dataSlice)
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading response for %s: %w", resp.Request.URL.String(), err)
	}
	if n != expectedBytes {
		return fmt.Errorf("downloaded %d bytes instead of %d for %s", n, expectedBytes, resp.Request.URL.String())
	}
	logger.Debug().Int("size", len(dataSlice)).Int("downloaded", n).Msg("downloaded chunk")
	return nil
}

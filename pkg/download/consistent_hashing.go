package download

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"runtime"
	"strconv"

	jump "github.com/dgryski/go-jump"
	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/logging"
	"golang.org/x/sync/errgroup"
)

type ConsistentHashingMode struct {
	Client *client.HTTPClient
	Options
}

func GetConsistentHashingMode(opts Options) (Strategy, error) {
	if opts.SliceSize == 0 {
		return nil, fmt.Errorf("Must specify slice size in consistent hashing mode")
	}
	client := client.NewHTTPClient(opts.Client)
	return &ConsistentHashingMode{
		Client:  client,
		Options: opts,
	}, nil
}

func (m *ConsistentHashingMode) maxChunks() int {
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

func (m *ConsistentHashingMode) Fetch(ctx context.Context, url string) (io.Reader, int64, error) {
	logger := logging.GetLogger()

	if m.Semaphore != nil {
		err := m.Semaphore.Acquire(ctx, 1)
		if err != nil {
			return nil, 0, err
		}
	}
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
	if fileSize < m.minChunkSize() {
		// we only need a single chunk: just download it and finish
		err = m.downloadChunk(firstChunkResp, data[0:fileSize])
		if err != nil {
			return nil, -1, err
		}
		return bytes.NewBuffer(data), fileSize, nil
	}

	chunkSize := (fileSize - m.minChunkSize()) / int64(m.maxChunks()-1)
	if chunkSize < m.minChunkSize() {
		chunkSize = m.minChunkSize()
	}
	if chunkSize < 0 {
		firstChunkResp.Body.Close()
		return nil, -1, fmt.Errorf("error: chunksize incorrect - result is negative, %d", chunkSize)
	}
	// not more than one connection per min chunk size
	chunks := int(math.Ceil(float64(fileSize) / float64(chunkSize)))

	if chunks > m.maxChunks() {
		chunks = m.maxChunks()
		chunkSize = fileSize / int64(chunks)
	}
	logger.Debug().Str("url", url).
		Int64("size", fileSize).
		Int("connections", chunks).
		Int64("chunkSize", chunkSize).
		Msg("Downloading")

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		return m.downloadChunk(firstChunkResp, data[0:m.minChunkSize()])
	})

	for i := 1; i < chunks; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1

		if i == chunks-1 {
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

func (m *ConsistentHashingMode) doRequest(ctx context.Context, start, end int64, trueURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", trueURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s: %w", req.URL.String(), err)
	}
	err = m.consistentHashIfNeeded(req, start/m.SliceSize, end/m.SliceSize)
	if err != nil {
		return nil, err
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

func (m *ConsistentHashingMode) consistentHashIfNeeded(req *http.Request, start int64, end int64) error {
	for _, host := range m.DomainsToCache {
		if host == req.URL.Host {
			if start/m.SliceSize != end/m.SliceSize {
				return fmt.Errorf("Can't make a range request across a slice boundary: %d-%d straddles a slice boundary (slice size is %d)", start, end, m.SliceSize)
			}
			slice := start / m.SliceSize

			key := fmt.Sprintf("%s#slice%d", req.URL, slice)
			hasher := fnv.New64a()
			hasher.Write([]byte(key))
			// jump is an implementation of Google's Jump Consistent Hash.
			//
			// See http://arxiv.org/abs/1406.2294 for details.
			cachePodIndex := int(jump.Hash(hasher.Sum64(), len(m.CacheHosts)))
			cacheHost := m.CacheHosts[cachePodIndex]
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
	return nil
}

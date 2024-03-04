package download_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/download"
)

var testFSes = []fstest.MapFS{
	{"hello.txt": {Data: []byte("0000000000000000")}},
	{"hello.txt": {Data: []byte("1111111111111111")}},
	{"hello.txt": {Data: []byte("2222222222222222")}},
	{"hello.txt": {Data: []byte("3333333333333333")}},
	{"hello.txt": {Data: []byte("4444444444444444")}},
	{"hello.txt": {Data: []byte("5555555555555555")}},
	{"hello.txt": {Data: []byte("6666666666666666")}},
	{"hello.txt": {Data: []byte("7777777777777777")}},
}

type chTestCase struct {
	name           string
	concurrency    int
	sliceSize      int64
	chunkSize      int64
	numCacheHosts  int
	expectedOutput string
}

// rangeResponder is an httpmock.Responder that implements enough of HTTP range
// requests for our purposes.
func rangeResponder(status int, body string) httpmock.Responder {
	rangeHeaderRegexp := regexp.MustCompile("^bytes=([0-9]+)-([0-9]+)$")
	return func(req *http.Request) (*http.Response, error) {
		rangeHeader := req.Header.Get("Range")
		if rangeHeader == "" {
			return httpmock.NewStringResponse(status, body), nil
		}
		rangePair := rangeHeaderRegexp.FindStringSubmatch(rangeHeader)
		if rangePair == nil {
			return httpmock.NewStringResponse(http.StatusBadRequest, "bad range header"), nil
		}
		from, err := strconv.Atoi(rangePair[1])
		if err != nil {
			return httpmock.NewStringResponse(http.StatusBadRequest, "bad range header"), nil
		}
		to, err := strconv.Atoi(rangePair[2])
		if err != nil {
			return httpmock.NewStringResponse(http.StatusBadRequest, "bad range header"), nil
		}
		// HTTP range header indexes are inclusive; we increment `to` so we have
		// inclusive from, exclusive to for use with slice ranges
		to++

		if from < 0 || from > to || from > len(body) || to < 0 {
			return httpmock.NewStringResponse(http.StatusRequestedRangeNotSatisfiable, "range unsatisfiable"), nil
		}
		if to > len(body) {
			to = len(body)
		}

		resp := httpmock.NewStringResponse(http.StatusPartialContent, body[from:to])
		resp.Request = req
		resp.Header.Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to-1, len(body)))
		resp.ContentLength = int64(to - from)
		resp.Header.Add("Content-Length", fmt.Sprint(resp.ContentLength))
		return resp, nil
	}
}

// fakeCacheHosts creates an *httpmock.MockTransport with preregistered
// responses to each of numberOfHosts distinct hostnames for the path
// /hello.txt.  The response will be bodyLength copies of a single character
// corresponding to the base-36 index of the cache host, starting 0-9, then a-z.
func fakeCacheHosts(numberOfHosts int, bodyLength int) (hostnames []string, transport *httpmock.MockTransport) {
	if numberOfHosts > 36 {
		panic("can't have more than 36 fake cache hosts, would overflow the base-36 body")
	}
	hostnames = make([]string, numberOfHosts)
	mockTransport := httpmock.NewMockTransport()

	for i := range hostnames {
		hostnames[i] = fmt.Sprintf("cache-host-%d", i)
		mockTransport.RegisterResponder("GET", fmt.Sprintf("http://%s/hello.txt", hostnames[i]),
			rangeResponder(200, strings.Repeat(strconv.FormatInt(int64(i), 36), bodyLength)))
	}
	return hostnames, mockTransport
}

var chTestCases = []chTestCase{
	{ // pre-computed demo that only some slices change as we add a new cache host
		name:           "1 host",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  1,
		chunkSize:      1,
		expectedOutput: "0000000000000000",
	},
	{
		name:           "2 hosts",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  2,
		chunkSize:      1,
		expectedOutput: "1111110000000000",
	},
	{
		name:           "3 hosts",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  3,
		chunkSize:      1,
		expectedOutput: "2221110000002222",
	},
	{
		name:           "4 hosts",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  4,
		chunkSize:      1,
		expectedOutput: "3331113333332222",
	},
	{
		name:           "5 hosts",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  5,
		chunkSize:      1,
		expectedOutput: "3334443333332224",
	},
	{
		name:           "6 hosts",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  6,
		chunkSize:      1,
		expectedOutput: "3334443333335554",
	},
	{
		name:           "7 hosts",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  7,
		chunkSize:      1,
		expectedOutput: "3334446666665556",
	},
	{
		name:           "8 hosts",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  8,
		chunkSize:      1,
		expectedOutput: "3334446666667776",
	},
	{
		name:           "test when fileSize % sliceSize == 0",
		concurrency:    8,
		sliceSize:      4,
		numCacheHosts:  8,
		chunkSize:      1,
		expectedOutput: "3333444466666666",
	},
	{
		name:           "when chunkSize == sliceSize",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  8,
		chunkSize:      3,
		expectedOutput: "3334446666667776",
	},
	{
		name:           "test when concurrency > file size",
		concurrency:    24,
		sliceSize:      3,
		numCacheHosts:  8,
		chunkSize:      3,
		expectedOutput: "3334446666667776",
	},
	{
		name:           "test when concurrency < number of slices",
		concurrency:    3,
		sliceSize:      3,
		numCacheHosts:  8,
		chunkSize:      3,
		expectedOutput: "3334446666667776",
	},
	{
		name:           "test when chunkSize == file size",
		concurrency:    4,
		sliceSize:      16,
		numCacheHosts:  8,
		chunkSize:      16,
		expectedOutput: "3333333333333333",
	},
	{
		name:           "test when chunkSize slightly below file size",
		concurrency:    4,
		sliceSize:      16,
		numCacheHosts:  8,
		chunkSize:      15,
		expectedOutput: "3333333333333333",
	},
	{
		name:           "test when chunkSize > file size",
		concurrency:    4,
		sliceSize:      24,
		numCacheHosts:  8,
		chunkSize:      24,
		expectedOutput: "3333333333333333",
	},
	{
		name:           "if chunkSize > sliceSize, sliceSize overrides it",
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  8,
		chunkSize:      24,
		expectedOutput: "3334446666667776",
	},
}

func makeCacheableURIPrefixes(uris ...string) map[string][]*url.URL {
	m := make(map[string][]*url.URL)
	for _, uri := range uris {
		parsed, err := url.Parse(uri)
		if err != nil {
			panic(err)
		}
		m[parsed.Host] = append(m[parsed.Host], parsed)
	}
	return m
}

func TestConsistentHashing(t *testing.T) {
	hostnames, mockTransport := fakeCacheHosts(8, 16)

	for _, tc := range chTestCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := download.Options{
				Client:               client.Options{Transport: mockTransport},
				MaxConcurrency:       tc.concurrency,
				ChunkSize:            tc.chunkSize,
				CacheHosts:           hostnames[0:tc.numCacheHosts],
				CacheableURIPrefixes: makeCacheableURIPrefixes("http://test.replicate.com"),
				SliceSize:            tc.sliceSize,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			strategy, err := download.GetConsistentHashingMode(opts)
			require.NoError(t, err)

			assert.Equal(t, tc.numCacheHosts, len(strategy.Options.CacheHosts))
			reader, _, err := strategy.Fetch(ctx, "http://test.replicate.com/hello.txt")
			require.NoError(t, err)
			bytes, err := io.ReadAll(reader)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedOutput, string(bytes))
		})
	}
}

func validatePathPrefixMiddleware(t *testing.T, next http.Handler, hostname string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, hostname, r.Host)
		hostPfx := fmt.Sprintf("/%s", hostname)
		assert.True(t, strings.HasPrefix(r.URL.Path, hostPfx))
		r.URL.Path = strings.TrimPrefix(r.URL.Path, hostPfx)
		next.ServeHTTP(w, r)
	})
}

func TestConsistentHashingPathBased(t *testing.T) {
	var hostname = "test.replicate.com"
	hostnames := make([]string, len(testFSes))
	for i, fs := range testFSes {
		validatePathPrefixAndStrip := validatePathPrefixMiddleware(t, http.FileServer(http.FS(fs)), hostname)
		ts := httptest.NewServer(validatePathPrefixAndStrip)
		defer ts.Close()
		url, err := url.Parse(ts.URL)
		require.NoError(t, err)
		hostnames[i] = url.Host
	}

	for _, tc := range chTestCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := download.Options{
				Client:               client.Options{},
				MaxConcurrency:       tc.concurrency,
				ChunkSize:            tc.chunkSize,
				CacheHosts:           hostnames[0:tc.numCacheHosts],
				CacheableURIPrefixes: makeCacheableURIPrefixes(fmt.Sprintf("http://%s", hostname)),
				CacheUsePathProxy:    true,
				SliceSize:            tc.sliceSize,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			strategy, err := download.GetConsistentHashingMode(opts)
			require.NoError(t, err)

			assert.Equal(t, tc.numCacheHosts, len(strategy.Options.CacheHosts))
			reader, _, err := strategy.Fetch(ctx, fmt.Sprintf("http://%s/hello.txt", hostname))
			require.NoError(t, err)
			bytes, err := io.ReadAll(reader)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedOutput, string(bytes))
		})
	}
}

func TestConsistentHashRetries(t *testing.T) {
	hostnames, mockTransport := fakeCacheHosts(8, 16)
	// deliberately "break" one cache host
	hostnames[0] = "broken-host"
	mockTransport.RegisterResponder("GET", "http://broken-host/hello.txt", httpmock.NewStringResponder(503, "fake broken host"))

	opts := download.Options{
		Client:               client.Options{Transport: mockTransport},
		MaxConcurrency:       8,
		ChunkSize:            1,
		CacheHosts:           hostnames,
		CacheableURIPrefixes: makeCacheableURIPrefixes("http://fake.replicate.delivery"),
		SliceSize:            1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	strategy, err := download.GetConsistentHashingMode(opts)
	require.NoError(t, err)

	reader, _, err := strategy.Fetch(ctx, "http://fake.replicate.delivery/hello.txt")
	require.NoError(t, err)
	bytes, err := io.ReadAll(reader)
	require.NoError(t, err)

	// with a functional hostnames[0], we'd see 0344760706165500, but instead we
	// should fall back to this. Note that each 0 value has been changed to a
	// different index; we don't want every request that previously hit 0 to hit
	// the same new host.
	assert.Equal(t, "3344761726165516", string(bytes))
}

func TestConsistentHashRetriesMissingHostname(t *testing.T) {
	hostnames, mockTransport := fakeCacheHosts(8, 16)

	// we deliberately "break" this cache host to make it as if its SRV record was missing
	hostnames[0] = ""

	opts := download.Options{
		Client: client.Options{
			Transport: mockTransport,
		},
		MaxConcurrency:       8,
		ChunkSize:            1,
		CacheHosts:           hostnames,
		CacheableURIPrefixes: makeCacheableURIPrefixes("http://fake.replicate.delivery"),
		SliceSize:            1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	strategy, err := download.GetConsistentHashingMode(opts)
	require.NoError(t, err)

	reader, _, err := strategy.Fetch(ctx, "http://fake.replicate.delivery/hello.txt")
	require.NoError(t, err)
	bytes, err := io.ReadAll(reader)
	require.NoError(t, err)

	// with a functional hostnames[0], we'd see 0344760706165500, but instead we
	// should fall back to this. Note that each 0 value has been changed to a
	// different index; we don't want every request that previously hit 0 to hit
	// the same new host.
	assert.Equal(t, "3344761726165516", string(bytes))
}

// with only two hosts, we should *always* fall back to the other host
func TestConsistentHashRetriesTwoHosts(t *testing.T) {
	hostnames, mockTransport := fakeCacheHosts(2, 16)
	// deliberately "break" one cache host
	hostnames[1] = "broken-host"
	mockTransport.RegisterResponder("GET", "http://broken-host/hello.txt", httpmock.NewStringResponder(503, "fake broken host"))

	opts := download.Options{
		Client:               client.Options{Transport: mockTransport},
		MaxConcurrency:       8,
		ChunkSize:            1,
		CacheHosts:           hostnames,
		CacheableURIPrefixes: makeCacheableURIPrefixes("http://testing.replicate.delivery"),
		SliceSize:            1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	strategy, err := download.GetConsistentHashingMode(opts)
	require.NoError(t, err)

	reader, _, err := strategy.Fetch(ctx, "http://testing.replicate.delivery/hello.txt")
	require.NoError(t, err)
	bytes, err := io.ReadAll(reader)
	require.NoError(t, err)

	assert.Equal(t, "0000000000000000", string(bytes))
}

func TestConsistentHashingHasFallback(t *testing.T) {
	mockTransport := httpmock.NewMockTransport()
	mockTransport.RegisterResponder("GET", "http://fake.replicate.delivery/hello.txt", rangeResponder(200, "0000000000000000"))

	opts := download.Options{
		Client:               client.Options{Transport: mockTransport},
		MaxConcurrency:       8,
		ChunkSize:            2,
		CacheHosts:           []string{""}, // simulate a single unavailable cache host
		CacheableURIPrefixes: makeCacheableURIPrefixes("http://fake.replicate.delivery"),
		SliceSize:            3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	strategy, err := download.GetConsistentHashingMode(opts)
	require.NoError(t, err)

	reader, _, err := strategy.Fetch(ctx, "http://fake.replicate.delivery/hello.txt")
	require.NoError(t, err)
	bytes, err := io.ReadAll(reader)
	require.NoError(t, err)

	assert.Equal(t, "0000000000000000", string(bytes))
}

type fallbackFailingHandler struct {
	responseStatus int
	responseFunc   func(w http.ResponseWriter, r *http.Request)
}

func (h fallbackFailingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.responseFunc != nil {
		h.responseFunc(w, r)
	} else {
		w.WriteHeader(h.responseStatus)
	}
}

type testStrategy struct {
	fetchCalledCount     int
	doRequestCalledCount int
	mut                  sync.Mutex
}

func (s *testStrategy) Fetch(ctx context.Context, url string) (io.Reader, int64, error) {
	s.fetchCalledCount++
	return io.NopCloser(strings.NewReader("00")), -1, nil
}

func (s *testStrategy) DoRequest(ctx context.Context, start, end int64, url string) (*http.Response, error) {
	s.mut.Lock()
	s.doRequestCalledCount++
	s.mut.Unlock()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp := &http.Response{
		Request: req,
		Body:    io.NopCloser(strings.NewReader("00")),
	}
	return resp, nil
}

func TestConsistentHashingFileFallback(t *testing.T) {
	tc := []struct {
		name                 string
		responseStatus       int
		failureFunc          func(w http.ResponseWriter, r *http.Request)
		fetchCalledCount     int
		doRequestCalledCount int
		expectedError        error
	}{
		{
			name:                 "BadGateway",
			responseStatus:       http.StatusBadGateway,
			fetchCalledCount:     1,
			doRequestCalledCount: 0,
		},
		// "NotFound" should not trigger fall-back
		{
			name:                 "NotFound",
			responseStatus:       http.StatusNotFound,
			fetchCalledCount:     0,
			doRequestCalledCount: 0,
			expectedError:        download.ErrUnexpectedHTTPStatus,
		},
	}

	for _, tc := range tc {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(fallbackFailingHandler{responseStatus: tc.responseStatus, responseFunc: tc.failureFunc})
			defer server.Close()

			url, _ := url.Parse(server.URL)
			opts := download.Options{
				Client:               client.Options{},
				MaxConcurrency:       8,
				ChunkSize:            2,
				CacheHosts:           []string{url.Host},
				CacheableURIPrefixes: makeCacheableURIPrefixes("http://fake.replicate.delivery"),
				SliceSize:            3,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			strategy, err := download.GetConsistentHashingMode(opts)
			require.NoError(t, err)

			fallbackStrategy := &testStrategy{}
			strategy.FallbackStrategy = fallbackStrategy

			urlString := "http://fake.replicate.delivery/hello.txt"
			_, _, err = strategy.Fetch(ctx, urlString)
			if tc.expectedError != nil {
				assert.ErrorIs(t, err, tc.expectedError)
			}
			assert.Equal(t, tc.fetchCalledCount, fallbackStrategy.fetchCalledCount)
			assert.Equal(t, tc.doRequestCalledCount, fallbackStrategy.doRequestCalledCount)
		})
	}
}

func TestConsistentHashingChunkFallback(t *testing.T) {
	handlerFunc := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=0-2" {
			w.WriteHeader(http.StatusBadGateway)
		} else {
			w.Header().Set("Content-Range", "bytes 0-2/4")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("000"))
		}
	}

	tc := []struct {
		name                 string
		responseStatus       int
		handlerFunc          func(w http.ResponseWriter, r *http.Request)
		fetchCalledCount     int
		doRequestCalledCount int
		expectedError        error
	}{
		{
			name:                 "fail-on-second-chunk",
			handlerFunc:          handlerFunc,
			fetchCalledCount:     0,
			doRequestCalledCount: 1,
		},
	}

	for _, tc := range tc {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(fallbackFailingHandler{responseStatus: tc.responseStatus, responseFunc: tc.handlerFunc})
			defer server.Close()

			url, _ := url.Parse(server.URL)
			opts := download.Options{
				Client:               client.Options{},
				MaxConcurrency:       8,
				ChunkSize:            3,
				CacheHosts:           []string{url.Host},
				CacheableURIPrefixes: makeCacheableURIPrefixes("http://fake.replicate.delivery"),
				SliceSize:            3,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			strategy, err := download.GetConsistentHashingMode(opts)
			require.NoError(t, err)

			fallbackStrategy := &testStrategy{}
			strategy.FallbackStrategy = fallbackStrategy

			urlString := "http://fake.replicate.delivery/hello.txt"
			out, _, err := strategy.Fetch(ctx, urlString)
			assert.ErrorIs(t, err, tc.expectedError)
			if err == nil {
				// eagerly read the whole output reader to force all the
				// requests to be completed
				_, _ = io.Copy(io.Discard, out)
			}
			assert.Equal(t, tc.fetchCalledCount, fallbackStrategy.fetchCalledCount)
			assert.Equal(t, tc.doRequestCalledCount, fallbackStrategy.doRequestCalledCount)
		})
	}
}

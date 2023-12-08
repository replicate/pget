package download_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

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

func makeConsistentHashingMode(opts download.Options) *download.ConsistentHashingMode {
	client := client.NewHTTPClient(opts.Client)
	fallbackMode := download.BufferMode{Options: opts, Client: client}

	return &download.ConsistentHashingMode{Client: client, Options: opts, FallbackStrategy: &fallbackMode}
}

type chTestCase struct {
	concurrency    int
	sliceSize      int64
	minChunkSize   int64
	numCacheHosts  int
	expectedOutput string
}

var chTestCases = []chTestCase{
	{ // pre-computed demo that only some slices change as we add a new cache host
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  1,
		minChunkSize:   1,
		expectedOutput: "0000000000000000",
	},
	{
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  2,
		minChunkSize:   1,
		expectedOutput: "1110000001111111",
	},
	{
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  3,
		minChunkSize:   1,
		expectedOutput: "1110000002222222",
	},
	{
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  4,
		minChunkSize:   1,
		expectedOutput: "1113330002222222",
	},
	{
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  5,
		minChunkSize:   1,
		expectedOutput: "1114440002222224",
	},
	{
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  6,
		minChunkSize:   1,
		expectedOutput: "1114440002222224",
	},
	{
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  7,
		minChunkSize:   1,
		expectedOutput: "1114440002226664",
	},
	{
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  8,
		minChunkSize:   1,
		expectedOutput: "1117770002226667",
	},
	{ // test when fileSize % sliceSize == 0
		concurrency:    8,
		sliceSize:      4,
		numCacheHosts:  8,
		minChunkSize:   1,
		expectedOutput: "1111777700002222",
	},
	{ // test when minChunkSize == sliceSize
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  8,
		minChunkSize:   3,
		expectedOutput: "1117770002226667",
	},
	{ // test when concurrency > file size
		concurrency:    24,
		sliceSize:      3,
		numCacheHosts:  8,
		minChunkSize:   3,
		expectedOutput: "1117770002226667",
	},
	{ // test when concurrency < number of slices
		concurrency:    3,
		sliceSize:      3,
		numCacheHosts:  8,
		minChunkSize:   3,
		expectedOutput: "1117770002226667",
	},
	{ // test when minChunkSize == file size
		concurrency:    4,
		sliceSize:      16,
		numCacheHosts:  8,
		minChunkSize:   16,
		expectedOutput: "1111111111111111",
	},
	{ // test when minChunkSize > file size
		concurrency:    4,
		sliceSize:      24,
		numCacheHosts:  8,
		minChunkSize:   24,
		expectedOutput: "1111111111111111",
	},
	{ // if minChunkSize > sliceSize, sliceSize overrides it
		concurrency:    8,
		sliceSize:      3,
		numCacheHosts:  8,
		minChunkSize:   24,
		expectedOutput: "1117770002226667",
	},
}

func TestConsistentHashing(t *testing.T) {
	hostnames := make([]string, len(testFSes))
	for i, fs := range testFSes {
		ts := httptest.NewServer(http.FileServer(http.FS(fs)))
		defer ts.Close()
		url, err := url.Parse(ts.URL)
		require.NoError(t, err)
		hostnames[i] = url.Host
	}

	for _, tc := range chTestCases {
		opts := download.Options{
			Client:         client.Options{},
			MaxConcurrency: tc.concurrency,
			MinChunkSize:   tc.minChunkSize,
			Semaphore:      semaphore.NewWeighted(int64(tc.concurrency)),
			CacheHosts:     hostnames[0:tc.numCacheHosts],
			DomainsToCache: []string{"fake.replicate.delivery"},
			SliceSize:      tc.sliceSize,
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		strategy := makeConsistentHashingMode(opts)

		reader, _, err := strategy.Fetch(ctx, "http://fake.replicate.delivery/hello.txt")
		assert.NoError(t, err)
		bytes, err := io.ReadAll(reader)
		assert.NoError(t, err)

		assert.Equal(t, tc.expectedOutput, string(bytes))
	}
}

func TestConsistentHashingHasFallback(t *testing.T) {
	server := httptest.NewServer(http.FileServer(http.FS(testFSes[0])))
	defer server.Close()

	opts := download.Options{
		Client:         client.Options{},
		MaxConcurrency: 8,
		MinChunkSize:   2,
		Semaphore:      semaphore.NewWeighted(8),
		CacheHosts:     []string{},
		DomainsToCache: []string{"fake.replicate.delivery"},
		SliceSize:      3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	strategy := makeConsistentHashingMode(opts)

	urlString, err := url.JoinPath(server.URL, "hello.txt")
	assert.NoError(t, err)
	reader, _, err := strategy.Fetch(ctx, urlString)
	assert.NoError(t, err)
	bytes, err := io.ReadAll(reader)
	assert.NoError(t, err)

	assert.Equal(t, "0000000000000000", string(bytes))
}

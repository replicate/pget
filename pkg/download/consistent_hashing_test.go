package download_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"testing/fstest"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/download"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
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

var consistentHashingOpts = download.Options{
	Client: client.Options{},
}

func makeConsistentHashingMode(opts download.Options) *download.ConsistentHashingMode {
	client := client.NewHTTPClient(opts.Client)

	return &download.ConsistentHashingMode{Client: client, Options: opts}
}

func TestConsistentHashing(t *testing.T) {
	opts := download.Options{
		Client:         client.Options{},
		MaxConcurrency: 8,
		MinChunkSize:   1,
		Semaphore:      semaphore.NewWeighted(4),
		DomainsToCache: []string{"fake.replicate.delivery"},
		SliceSize:      3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hostnames := make([]string, len(testFSes))
	for i, fs := range testFSes {
		ts := httptest.NewServer(http.FileServer(http.FS(fs)))
		defer ts.Close()
		url, err := url.Parse(ts.URL)
		require.NoError(t, err)
		hostnames[i] = url.Host
	}
	opts.CacheHosts = hostnames

	strategy := makeConsistentHashingMode(opts)

	reader, _, err := strategy.Fetch(ctx, "http://fake.replicate.delivery/hello.txt")
	assert.NoError(t, err)
	bytes, err := io.ReadAll(reader)
	assert.NoError(t, err)

	assert.Equal(t, "1117770002226667", string(bytes))
}

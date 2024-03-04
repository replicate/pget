package download

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"testing/fstest"

	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/replicate/pget/pkg/client"
)

func init() {
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
}

const testFilePath = "test.txt"

// generateTestContent generates a byte slice of a random size > 1KiB
func generateTestContent(size int64) []byte {
	content := make([]byte, size)
	// Generate random bytes and write them to the content slice
	for i := range content {
		content[i] = byte(rand.Intn(256))
	}
	return content

}

// newTestServer creates a new http server that serves the given content
func newTestServer(t *testing.T, content []byte) *httptest.Server {
	testFileSystem := fstest.MapFS{testFilePath: {Data: content}}
	server := httptest.NewServer(http.FileServer(http.FS(testFileSystem)))
	return server
}

// TODO: Implement the test
// func TestGetFileSizeFromContentRange(t *testing.T) {}
func TestFileToBufferChunkCountExceedsMaxChunks(t *testing.T) {
	contentSize := int64(humanize.KByte)
	content := generateTestContent(contentSize)
	server := newTestServer(t, content)
	defer server.Close()
	opts := Options{
		Client: client.Options{},
	}
	// Ensure that the math generally works out as such for this test case where chunkSize is < 0.5* contentSize
	// (contentSize - chunkSize) / chunkSize < maxChunks
	// This ensures that we're always testing the case where the number of chunks exceeds the maxChunks
	// Additional cases added to validate various cases where the final chunk is less than chunkSize
	tc := []struct {
		name           string
		maxConcurrency int
		chunkSize      int64
	}{
		// In these first cases we will never have more than 2 chunks as the chunkSize is greater than 0.5*contentSize
		{
			name:           "chunkSize greater than contentSize",
			chunkSize:      contentSize + 1,
			maxConcurrency: 1,
		},
		{
			name:           "chunkSize equal to contentSize",
			chunkSize:      contentSize,
			maxConcurrency: 1,
		},
		{
			name:           "chunkSize less than contentSize",
			chunkSize:      contentSize - 1,
			maxConcurrency: 2,
		},
		{
			name:           "chunkSize is 3/4 contentSize",
			chunkSize:      int64(float64(contentSize) * 0.75),
			maxConcurrency: 2,
		},
		{
			// This is an exceptional case where we only need a single additional chunk beyond the default "get content size"
			// request.
			name:           "chunkSize is 1/2 contentSize",
			chunkSize:      int64(float64(contentSize) * 0.5),
			maxConcurrency: 2,
		},
		// These test cases cover a few scenarios of downloading where the maxChunks will force a re-calculation of
		// the chunkSize to ensure that we don't exceed the maxChunks.
		{
			// remainder will result in 3 chunks, max-chunks is 2
			name:           "chunkSize is 1/4 contentSize",
			chunkSize:      int64(float64(contentSize) * 0.25),
			maxConcurrency: 2,
		},
		{
			// humanize.KByte = 1024, remainder will result in 1024/10 = 102 chunks, max-chunks is set to 25
			// resulting in a chunkSize of 41
			name:           "many chunks, low maxChunks",
			chunkSize:      10,
			maxConcurrency: 25,
		},
	}

	for _, tc := range tc {
		t.Run(tc.name, func(t *testing.T) {
			opts.MaxConcurrency = tc.maxConcurrency
			opts.ChunkSize = tc.chunkSize
			bufferMode := GetBufferMode(opts)
			path, _ := url.JoinPath(server.URL, testFilePath)
			download, size, err := bufferMode.Fetch(context.Background(), path)
			require.NoError(t, err)
			data, err := io.ReadAll(download)
			assert.NoError(t, err)
			assert.Equal(t, contentSize, size)
			assert.Equal(t, len(content), len(data))
			assert.Equal(t, content, data)
		})
	}
}

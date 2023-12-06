package download

import (
	"context"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"testing/fstest"

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
	// Ensure that the math generally works out as such for this test case where minChunkSize is < 0.5* contentSize
	// (contentSize - minChunkSize) / minChunkSize < maxChunks
	// This ensures that we're always testing the case where the number of chunks exceeds the maxChunks
	// Additional cases added to validate various cases where the final chunk is less than minChunkSize
	tc := []struct {
		name         string
		maxChunks    int
		minChunkSize int64
	}{
		// In these first cases we will never have more than 2 chunks as the minChunkSize is greater than 0.5*contentSize
		{
			name:         "minChunkSize greater than contentSize",
			minChunkSize: contentSize + 1,
			maxChunks:    1,
		},
		{
			name:         "minChunkSize equal to contentSize",
			minChunkSize: contentSize,
			maxChunks:    1,
		},
		{
			name:         "minChunkSize less than contentSize",
			minChunkSize: contentSize - 1,
			maxChunks:    2,
		},
		{
			name:         "minChunkSize is 3/4 contentSize",
			minChunkSize: int64(float64(contentSize) * 0.75),
			maxChunks:    2,
		},
		{
			// This is an exceptional case where we only need a single additional chunk beyond the default "get content size"
			// request.
			name:         "minChunkSize is 1/2 contentSize",
			minChunkSize: int64(float64(contentSize) * 0.5),
			maxChunks:    2,
		},
		// These test cases cover a few scenarios of downloading where the maxChunks will force a re-calculation of
		// the chunkSize to ensure that we don't exceed the maxChunks.
		{
			// remainder will result in 3 chunks, max-chunks is 2
			name:         "minChunkSize is 1/4 contentSize",
			minChunkSize: int64(float64(contentSize) * 0.25),
			maxChunks:    2,
		},
		{
			// humanize.KByte = 1024, remainder will result in 1024/10 = 102 chunks, max-chunks is set to 25
			// resulting in a chunkSize of 41
			name:         "many chunks, low maxChunks",
			minChunkSize: 10,
			maxChunks:    25,
		},
	}

	for _, tc := range tc {
		t.Run(tc.name, func(t *testing.T) {
			opts.MaxChunks = tc.maxChunks
			opts.MinChunkSize = tc.minChunkSize
			bufferMode := makeBufferMode(opts)
			path, _ := url.JoinPath(server.URL, testFilePath)
			download, size, err := bufferMode.fileToBuffer(context.Background(), path)
			assert.NoError(t, err)
			assert.Equal(t, contentSize, size)
			assert.Equal(t, len(content), download.Len())
			assert.Equal(t, content, download.Bytes())
		})
	}
}

func makeBufferMode(opts Options) *BufferMode {
	client := client.NewHTTPClient(opts.Client)

	return &BufferMode{Client: client, Options: opts}
}

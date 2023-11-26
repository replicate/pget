package download_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"testing/fstest"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/download"
	"github.com/stretchr/testify/assert"
)

var testFS = fstest.MapFS{
	"hello.txt": {Data: []byte("hello, world!")},
}

func makeBufferMode() *download.BufferMode {
	clientOpts := client.Options{
		MaxConnPerHost: 40,
		ForceHTTP2:     false,
		MaxRetries:     2,
	}
	client := client.NewHTTPClient(clientOpts)

	return &download.BufferMode{Client: client}
}

func tempFilename() string {
	// get a temp filename that doesn't already exist by creating
	// a temp file and immediately deleting it
	dest, _ := os.CreateTemp("", "pget-buffer-test")
	os.Remove(dest.Name())
	return dest.Name()
}

func assertFileHasContent(t *testing.T, expectedContent []byte, path string) {
	contentFile, err := os.Open(path)
	assert.NoError(t, err)
	defer contentFile.Close()

	content, err := io.ReadAll(contentFile)
	assert.NoError(t, err)

	assert.Equal(t, expectedContent, content)
}

func TestDownloadSmallFile(t *testing.T) {
	ts := httptest.NewServer(http.FileServer(http.FS(testFS)))
	defer ts.Close()

	dest := tempFilename()
	defer os.Remove(dest)

	bufferMode := makeBufferMode()

	_, _, err := bufferMode.DownloadFile(ts.URL+"/hello.txt", dest)
	assert.NoError(t, err)

	assert.FileExists(t, dest)
	assertFileHasContent(t, testFS["hello.txt"].Data, dest)
}

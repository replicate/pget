package pget_test

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"testing/fstest"
	"testing/iotest"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pget "github.com/replicate/pget/pkg"
	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/download"
)

var testFS = fstest.MapFS{
	"hello.txt": {Data: []byte("hello, world!")},
}

func init() {
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
}

var defaultOpts = download.Options{Client: client.Options{}}
var http2Opts = download.Options{Client: client.Options{ForceHTTP2: true}}

func makeGetter(opts download.Options) *pget.Getter {
	client := client.NewHTTPClient(opts.Client)

	return &pget.Getter{
		Downloader: &download.BufferMode{Client: client, Options: opts},
	}
}

func tempFilename() string {
	// get a temp filename that doesn't already exist by creating
	// a temp file and immediately deleting it
	dest, _ := os.CreateTemp("", "pget-buffer-test")
	os.Remove(dest.Name())
	return dest.Name()
}

// writeRandomFile creates a sparse file with the given size and
// writes some random bytes somewhere in it.  This is much faster than
// filling the whole file with random bytes would be, but it also
// gives us some confidence that the range requests are being
// reassembled correctly.
func writeRandomFile(t require.TestingT, path string, size int64) {
	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	rnd := rand.New(rand.NewSource(99))

	// under 1 MiB, just fill the whole file with random data
	if size < 1024*1024 {
		_, err = io.CopyN(file, rnd, size)
		require.NoError(t, err)
		return
	}

	// set the file size
	err = file.Truncate(size)
	require.NoError(t, err)

	// write some random data to the start
	_, err = io.CopyN(file, rnd, 1024)
	require.NoError(t, err)

	// and somewhere else in the file
	_, err = file.Seek(rnd.Int63()%(size-1024), io.SeekStart)
	require.NoError(t, err)
	_, err = io.CopyN(file, rnd, 1024)
	require.NoError(t, err)
}

func assertFileHasContent(t *testing.T, expectedContent []byte, path string) {
	contentFile, err := os.Open(path)
	require.NoError(t, err)
	defer contentFile.Close()

	assert.NoError(t, iotest.TestReader(contentFile, expectedContent))
}

func TestDownloadSmallFile(t *testing.T) {
	ts := httptest.NewServer(http.FileServer(http.FS(testFS)))
	defer ts.Close()

	dest := tempFilename()
	defer os.Remove(dest)

	getter := makeGetter(defaultOpts)

	_, _, err := getter.DownloadFile(context.Background(), ts.URL+"/hello.txt", dest)
	assert.NoError(t, err)

	assertFileHasContent(t, testFS["hello.txt"].Data, dest)
}

func testDownloadSingleFile(opts download.Options, size int64, t *testing.T) {
	dir, err := os.MkdirTemp("", "pget-buffer-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	srcFilename := filepath.Join(dir, "random-bytes")

	writeRandomFile(t, srcFilename, size)

	ts := httptest.NewServer(http.FileServer(http.Dir(dir)))
	defer ts.Close()

	getter := makeGetter(opts)

	dest := tempFilename()
	defer os.Remove(dest)

	_, _, err = getter.DownloadFile(context.Background(), ts.URL+"/random-bytes", dest)
	assert.NoError(t, err)

	cmd := exec.Command("diff", "-q", srcFilename, dest)
	err = cmd.Run()
	assert.NoError(t, err, "source file and dest file should be identical")
}

func TestDownload10MH1(t *testing.T)  { testDownloadSingleFile(defaultOpts, 10*1024*1024, t) }
func TestDownload100MH1(t *testing.T) { testDownloadSingleFile(defaultOpts, 100*1024*1024, t) }
func TestDownload10MH2(t *testing.T)  { testDownloadSingleFile(http2Opts, 10*1024*1024, t) }
func TestDownload100MH2(t *testing.T) { testDownloadSingleFile(http2Opts, 100*1024*1024, t) }

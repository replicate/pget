package multifile

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/optname"
)

type dummyModeCallerArgs struct {
	url  string
	dest string
}

type dummyMode struct {
	args      []dummyModeCallerArgs
	returnErr bool
	calls     chan dummyModeCallerArgs
}

// ensure *dummyMode implements download.Mode
var _ download.Mode = &dummyMode{}

func (d *dummyMode) DownloadFile(url string, dest string) (int64, time.Duration, error) {
	d.calls <- dummyModeCallerArgs{url, dest}
	if d.returnErr {
		return -1, time.Duration(0), errors.New("test error")
	}
	return 100, time.Duration(1) * time.Second, nil
}

// Args returns the args that DownloadFile was called with.
func (d *dummyMode) Args() []dummyModeCallerArgs {
DONE:
	// non-blocking read the whole channel into d.args
	for {
		select {
		case args := <-d.calls:
			d.args = append(d.args, args)
		default:
			break DONE
		}
	}
	return d.args
}

func (d *dummyMode) Arg(i int) dummyModeCallerArgs {
	return d.Args()[i]
}

func randomName() string {
	charset := "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 10)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// setupDummyMode registers a dummy mode with the download package and returns the dummymode name
// and a cleanup function to be called after the test is done
func setupDummyMode(returnErr bool) (string, *dummyMode, func()) {
	modeName := randomName()
	dummy := &dummyMode{
		returnErr: returnErr,
		calls:     make(chan dummyModeCallerArgs, 100),
	}
	cleanupFunc, err := download.AddMode(modeName, func() download.Mode { return dummy })
	if err != nil {
		panic(err)
	}
	return modeName, dummy, cleanupFunc
}

func resetPostTest() {
	downloadMetrics = []multifileDownloadMetric{}
	config.Mode = "buffer"
}

func TestDownloadFilesFromHost(t *testing.T) {
	modeName, mode, cleanupFunc := setupDummyMode(false)
	defer cleanupFunc()
	defer resetPostTest()

	entries := []manifestEntry{
		{"https://example.com/file1.txt", "/tmp/file1.txt"},
		{"https://example.com/file2.txt", "/tmp/file2.txt"},
	}
	var eg errgroup.Group
	config.Mode = modeName
	_ = downloadFilesFromHost(&eg, entries)
	err := eg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(mode.Args()))
	assert.Contains(t, mode.Args(), dummyModeCallerArgs{entries[0].url, entries[0].dest})
	assert.Contains(t, mode.Args(), dummyModeCallerArgs{entries[1].url, entries[1].dest})

	failsModeName, _, failsCleanupFunc := setupDummyMode(true)
	defer failsCleanupFunc()

	eg = errgroup.Group{}
	config.Mode = failsModeName
	_ = downloadFilesFromHost(&eg, entries)
	err = eg.Wait()
	assert.Error(t, err)

}

func TestDownloadAndMeasure(t *testing.T) {
	_, mode, cleanupFunc := setupDummyMode(false)
	defer cleanupFunc()
	defer resetPostTest()

	url := "https://example.com/file1.txt"
	dest := "/tmp/file1.txt"
	err := downloadAndMeasure(mode, url, dest)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mode.Args()))
	assert.Equal(t, url, mode.Arg(0).url)
	assert.Equal(t, dest, mode.Arg(0).dest)
}

func TestAddDownloadMetrics(t *testing.T) {
	defer resetPostTest()
	elapsedTime := time.Duration(1) * time.Second
	fileSize := int64(100)
	addDownloadMetrics(elapsedTime, fileSize)
	assert.Equal(t, 1, len(downloadMetrics))
	assert.Equal(t, elapsedTime, downloadMetrics[0].elapsedTime)
	assert.Equal(t, fileSize, downloadMetrics[0].fileSize)
}

func TestMultifilePreRunE(t *testing.T) {
	defer resetPostTest()
	cmd := GetCommand()
	config.AddFlags(cmd)

	// Test that extract cannot be set at the same time as multifile is used
	viper.Set(optname.Extract, true)
	err := multifilePreRunE(cmd, []string{})
	assert.Error(t, err)

	viper.Reset()
}

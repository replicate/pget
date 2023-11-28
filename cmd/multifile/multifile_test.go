package multifile

import (
	"context"
	"errors"
	"sync"
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

func (d *dummyMode) DownloadFile(ctx context.Context, url string, dest string) (int64, time.Duration, error) {
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

// initializeDummyMode returns a download.Mode that returns an error if returnErr is true
// This function returns a download.Mode instead of a *dummyMode so that we can ensure
// that *dummyMode implements download.Mode
func initializeDummyMode(returnErr bool) download.Mode {
	dummy := &dummyMode{
		returnErr: returnErr,
		calls:     make(chan dummyModeCallerArgs, 100),
	}
	return dummy
}

// getDummyMode returns a dummyMode wrapping initializeDummyMode
// tests should use getDummyMode instead of initializeDummyMode
func getDummyMode(returnErr bool) *dummyMode {
	return initializeDummyMode(returnErr).(*dummyMode)
}

func TestDownloadFilesFromHost(t *testing.T) {
	mode := getDummyMode(false)

	entries := []manifestEntry{
		{"https://example.com/file1.txt", "/tmp/file1.txt"},
		{"https://example.com/file2.txt", "/tmp/file2.txt"},
	}

	metrics := &downloadMetrics{
		mut: sync.Mutex{},
	}

	errGroup, ctx := errgroup.WithContext(context.Background())
	_ = downloadFilesFromHost(ctx, mode, errGroup, entries, metrics)
	err := errGroup.Wait()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(mode.Args()))
	assert.Contains(t, mode.Args(), dummyModeCallerArgs{entries[0].url, entries[0].dest})
	assert.Contains(t, mode.Args(), dummyModeCallerArgs{entries[1].url, entries[1].dest})

	failsMode := getDummyMode(true)

	errGroup, ctx = errgroup.WithContext(context.Background())
	_ = downloadFilesFromHost(ctx, failsMode, errGroup, entries, metrics)
	err = errGroup.Wait()
	_ = failsMode.Args()
	assert.Error(t, err)
}

func TestDownloadAndMeasure(t *testing.T) {

	mode := getDummyMode(false)

	metrics := &downloadMetrics{
		mut: sync.Mutex{},
	}

	url := "https://example.com/file1.txt"
	dest := "/tmp/file1.txt"
	err := downloadAndMeasure(context.Background(), mode, url, dest, metrics)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mode.Args()))
	assert.Equal(t, url, mode.Arg(0).url)
	assert.Equal(t, dest, mode.Arg(0).dest)
}

func TestAddDownloadMetrics(t *testing.T) {
	metrics := &downloadMetrics{
		metrics: []multifileDownloadMetric{},
		mut:     sync.Mutex{},
	}

	elapsedTime := time.Duration(1) * time.Second
	fileSize := int64(100)
	addDownloadMetrics(elapsedTime, fileSize, metrics)
	assert.Equal(t, 1, len(metrics.metrics))
	assert.Equal(t, elapsedTime, metrics.metrics[0].elapsedTime)
	assert.Equal(t, fileSize, metrics.metrics[0].fileSize)
}

func TestMultifilePreRunE(t *testing.T) {
	cmd := GetCommand()
	if err := config.AddRootPersistentFlags(cmd); err != nil {
		t.Fatal(err)
	}

	// Test that extract cannot be set at the same time as multifile is used
	viper.Set(optname.Extract, true)
	err := multifilePreRunE(cmd, []string{})
	assert.Error(t, err)

	viper.Reset()
}

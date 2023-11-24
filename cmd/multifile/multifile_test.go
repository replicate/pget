package multifile

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/optname"
)

type dummyModeCallerArgs struct {
	url  string
	dest string
}

type dummyMode struct {
	timesCalled int
	args        []dummyModeCallerArgs
	returnErr   bool
}

// ensure *dummyMode implements download.Mode
var _ download.Mode = &dummyMode{}

func (d *dummyMode) DownloadFile(url string, dest string) (int64, time.Duration, error) {
	d.timesCalled++
	d.args = append(d.args, dummyModeCallerArgs{url, dest})
	if d.returnErr {
		return -1, time.Duration(0), errors.New("test error")
	}
	return 100, time.Duration(1) * time.Second, nil
}

func TestDownloadFilesFromHost(t *testing.T) {
	mode := &dummyMode{returnErr: false}

	entries := []manifestEntry{
		{"https://example.com/file1.txt", "/tmp/file1.txt"},
		{"https://example.com/file2.txt", "/tmp/file2.txt"},
	}

	metrics := &downhloadMetrics{
		mut: sync.Mutex{},
	}

	eg := initializeErrGroup()
	_ = downloadFilesFromHost(mode, eg, entries, metrics)
	err := eg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, 2, mode.timesCalled)
	assert.Contains(t, mode.args, dummyModeCallerArgs{entries[0].url, entries[0].dest})
	assert.Contains(t, mode.args, dummyModeCallerArgs{entries[1].url, entries[1].dest})

	failsMode := &dummyMode{returnErr: true}

	eg = initializeErrGroup()
	_ = downloadFilesFromHost(failsMode, eg, entries, metrics)
	err = eg.Wait()
	assert.Error(t, err)

}

func TestDownloadAndMeasure(t *testing.T) {

	mode := &dummyMode{returnErr: false}

	metrics := &downhloadMetrics{
		mut: sync.Mutex{},
	}

	url := "https://example.com/file1.txt"
	dest := "/tmp/file1.txt"
	err := downloadAndMeasure(mode, url, dest, metrics)
	assert.NoError(t, err)
	assert.Equal(t, 1, mode.timesCalled)
	assert.Equal(t, url, mode.args[0].url)
	assert.Equal(t, dest, mode.args[0].dest)
}

func TestAddDownloadMetrics(t *testing.T) {
	metrics := &downhloadMetrics{
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

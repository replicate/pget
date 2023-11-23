package multifile

import (
	"errors"
	"math/rand"
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

func (d *dummyMode) DownloadFile(url string, dest string) (int64, time.Duration, error) {
	d.timesCalled++
	d.args = append(d.args, dummyModeCallerArgs{url, dest})
	if d.returnErr {
		return -1, time.Duration(0), errors.New("test error")
	}
	return 100, time.Duration(1) * time.Second, nil
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
func setupDummyMode(returnErr bool) (string, download.Mode, func()) {
	modeName := randomName()
	dummy := &dummyMode{returnErr: returnErr}
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
	eg := initializeErrGroup()
	config.Mode = modeName
	_ = downloadFilesFromHost(eg, entries)
	err := eg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, 2, mode.(*dummyMode).timesCalled)
	assert.Contains(t, mode.(*dummyMode).args, dummyModeCallerArgs{entries[0].url, entries[0].dest})
	assert.Contains(t, mode.(*dummyMode).args, dummyModeCallerArgs{entries[1].url, entries[1].dest})

	failsModeName, _, failsCleanupFunc := setupDummyMode(true)
	defer failsCleanupFunc()

	eg = initializeErrGroup()
	config.Mode = failsModeName
	_ = downloadFilesFromHost(eg, entries)
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
	assert.Equal(t, 1, mode.(*dummyMode).timesCalled)
	assert.Equal(t, url, mode.(*dummyMode).args[0].url)
	assert.Equal(t, dest, mode.(*dummyMode).args[0].dest)
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

	// Test default values and that default 40 is set if no value is provided
	assert.Equal(t, viper.GetInt(optname.MaxConnPerHost), 0)
	err := multifilePreRunE(cmd, []string{})
	assert.NoError(t, err)
	assert.Equal(t, viper.GetInt(optname.MaxConnPerHost), 40)

	viper.Reset()

	// Test that the value is not overridden by the value of 40 if provided
	viper.Set(optname.MaxConnPerHost, 10)
	err = multifilePreRunE(cmd, []string{})
	assert.NoError(t, err)
	assert.Equal(t, viper.GetInt(optname.MaxConnPerHost), 10)

	viper.Reset()

	// Test that extract cannot be set at the same time as multifile is used
	viper.Set(optname.Extract, true)
	err = multifilePreRunE(cmd, []string{})
	assert.Error(t, err)

	viper.Reset()
}

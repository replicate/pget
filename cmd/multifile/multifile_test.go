package multifile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

type dummyGetterCallerArgs struct {
	url  string
	dest string
}

type dummyGetter struct {
	args      []dummyGetterCallerArgs
	returnErr bool
	calls     chan dummyGetterCallerArgs
}

// ensure *dummyGetter implements Getter
var _ Getter = &dummyGetter{}

func (d *dummyGetter) DownloadFile(ctx context.Context, url string, dest string) (int64, time.Duration, error) {
	d.calls <- dummyGetterCallerArgs{url, dest}
	if d.returnErr {
		return -1, time.Duration(0), errors.New("test error")
	}
	return 100, time.Duration(1) * time.Second, nil
}

// Args returns the args that DownloadFile was called with.
func (d *dummyGetter) Args() []dummyGetterCallerArgs {
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

func (d *dummyGetter) Arg(i int) dummyGetterCallerArgs {
	return d.Args()[i]
}

func getDummyGetter(returnErr bool) *dummyGetter {
	return &dummyGetter{
		returnErr: returnErr,
		calls:     make(chan dummyGetterCallerArgs, 100),
	}
}

func TestDownloadFilesFromHost(t *testing.T) {
	getter := getDummyGetter(false)

	entries := []manifestEntry{
		{"https://example.com/file1.txt", "/tmp/file1.txt"},
		{"https://example.com/file2.txt", "/tmp/file2.txt"},
	}

	metrics := &downloadMetrics{
		mut: sync.Mutex{},
	}

	errGroup, ctx := errgroup.WithContext(context.Background())
	_ = downloadFilesFromHost(ctx, getter, errGroup, entries, metrics)
	err := errGroup.Wait()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(getter.Args()))
	assert.Contains(t, getter.Args(), dummyGetterCallerArgs{entries[0].url, entries[0].dest})
	assert.Contains(t, getter.Args(), dummyGetterCallerArgs{entries[1].url, entries[1].dest})

	failsGetter := getDummyGetter(true)

	errGroup, ctx = errgroup.WithContext(context.Background())
	_ = downloadFilesFromHost(ctx, failsGetter, errGroup, entries, metrics)
	err = errGroup.Wait()
	_ = failsGetter.Args()
	assert.Error(t, err)
}

func TestDownloadAndMeasure(t *testing.T) {

	getter := getDummyGetter(false)

	metrics := &downloadMetrics{
		mut: sync.Mutex{},
	}

	url := "https://example.com/file1.txt"
	dest := "/tmp/file1.txt"
	err := downloadAndMeasure(context.Background(), getter, url, dest, metrics)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(getter.Args()))
	assert.Equal(t, url, getter.Arg(0).url)
	assert.Equal(t, dest, getter.Arg(0).dest)
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

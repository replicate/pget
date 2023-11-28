package download

import (
	"context"
	"fmt"
	"time"

	"github.com/replicate/pget/pkg/client"
)

type modeFactory func(opts Options) Mode

type Mode interface {
	DownloadFile(ctx context.Context, url string, dest string) (fileSize int64, elapsedTime time.Duration, err error)
}

type Options struct {

	// Maximum number of chunks to download. If set to zero, GOMAXPROCS*4
	// will be used.
	MaxChunks int

	// Minimum number of bytes per chunk. If set to zero, 16 MiB will be
	// used.
	MinChunkSize int64
	Client       client.Options
}

func modeFactories() map[string]modeFactory {
	return map[string]modeFactory{
		BufferModeName:     getBufferMode,
		ExtractTarModeName: getExtractTarMode,
	}
}

func GetMode(name string, opts Options) (Mode, error) {
	factory, ok := modeFactories()[name]
	if !ok {
		return nil, fmt.Errorf("unknown mode: %s", name)
	}
	return factory(opts), nil
}

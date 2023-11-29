package download

import (
	"context"
	"time"

	"github.com/replicate/pget/pkg/client"
)

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

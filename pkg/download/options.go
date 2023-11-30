package download

import (
	"github.com/replicate/pget/pkg/client"
)

type Options struct {
	// Maximum number of chunks to download. If set to zero, GOMAXPROCS*4
	// will be used.
	MaxConcurrency int

	// Minimum number of bytes per chunk. If set to zero, 16 MiB will be
	// used.
	MinChunkSize int64
	Client       client.Options
}

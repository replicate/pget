package download

import (
	"golang.org/x/sync/semaphore"

	"github.com/replicate/pget/pkg/client"
)

type Options struct {
	// Maximum number of chunks to download. If set to zero, GOMAXPROCS*4
	// will be used.
	MaxConcurrency int

	// SliceSize is the number of bytes per slice in nginx.
	// See https://nginx.org/en/docs/http/ngx_http_slice_module.html
	SliceSize int64

	// Minimum number of bytes per chunk. If set to zero, 16 MiB will be
	// used.
	MinChunkSize int64
	Client       client.Options

	// DomainsToCache is an allowlist of domains which may be routed via a
	// pull-through cache
	DomainsToCache []string

	// CacheHosts is a slice of hostnames to use as pull-through caches.
	// The ordering is significant and will be used with the consistent
	// hashing algorithm.  The slice may contain empty entries which
	// correspond to a cache host which is currently unavailable.
	CacheHosts []string

	// Semaphore is used to manage maximum concurrency. If nil, concurrency
	// is unlimited.
	Semaphore *semaphore.Weighted
}

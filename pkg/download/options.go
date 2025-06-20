package download

import (
	"net/url"
	"runtime"

	"github.com/replicate/pget/pkg/client"
)

type Options struct {
	// Maximum number of chunks to download. If set to zero, GOMAXPROCS*4
	// will be used.
	MaxConcurrency int

	// SliceSize is the number of bytes per slice in nginx.
	// See https://nginx.org/en/docs/http/ngx_http_slice_module.html
	SliceSize int64

	// Number of bytes per chunk. If set to zero, 125 MiB will be used.
	ChunkSize int64

	Client client.Options

	// CacheableURIPrefixes is an allowlist of domains+path-prefixes which may
	// be routed via a pull-through cache
	CacheableURIPrefixes map[string][]*url.URL

	// CacheUsePathProxy is a flag to indicate whether to use the path proxy mechanism or the host-based mechanism
	// The default is to use the host-based mechanism, the path proxy mechanism is used when this flag is set to true
	// and involves prepending the host to the path of the request to the cache. In both cases the Hosts header is
	// sent to the cache.
	CacheUsePathProxy bool

	// CacheHosts is a slice of hostnames to use as pull-through caches.
	// The ordering is significant and will be used with the consistent
	// hashing algorithm.  The slice may contain empty entries which
	// correspond to a cache host which is currently unavailable.
	CacheHosts []string

	// ForceCachePrefixRewrite will forcefully rewrite the prefix for all
	// pget requests to the first item in the CacheHosts list. This ignores
	// anything in the CacheableURIPrefixes and rewrites all requests.
	ForceCachePrefixRewrite bool
}

func (o *Options) maxConcurrency() int {
	maxChunks := o.MaxConcurrency
	if maxChunks == 0 {
		return runtime.NumCPU() * 4
	}
	return maxChunks
}

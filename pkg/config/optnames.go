package config

const (
	// these options are a massive hack. They're only availabe via
	// envvar, not command line
	OptCacheNodesSRVNameByHostCIDR = "cache-nodes-srv-name-by-host-cidr"
	OptCacheNodesSRVName           = "cache-nodes-srv-name"
	OptCacheURIPrefixes            = "cache-uri-prefixes"
	OptCacheUsePathProxy           = "cache-use-path-proxy"
	OptHostIP                      = "host-ip"

	// Normal options with CLI arguments
	OptConcurrency        = "concurrency"
	OptConnTimeout        = "connect-timeout"
	OptExtract            = "extract"
	OptForce              = "force"
	OptForceHTTP2         = "force-http2"
	OptLoggingLevel       = "log-level"
	OptMaxChunks          = "max-chunks"
	OptMaxConnPerHost     = "max-conn-per-host"
	OptMaxConcurrentFiles = "max-concurrent-files"
	OptMinimumChunkSize   = "minimum-chunk-size"
	OptOutputConsumer     = "output"
	OptPIDFile            = "pid-file"
	OptResolve            = "resolve"
	OptRetries            = "retries"
	OptVerbose            = "verbose"
)

package config

const (
	// these two options are a massive hack. They're only availabe via
	// envvar, not command line
	OptCacheNodesSRVNameByHostCIDR = "cache-nodes-srv-name-by-host-cidr"
	OptHostIP                      = "host-ip"

	OptCacheNodesSRVName = "cache-nodes-srv-name"
	OptConcurrency       = "concurrency"
	OptConnTimeout       = "connect-timeout"
	OptExtract           = "extract"
	OptForce             = "force"
	OptForceHTTP2        = "force-http2"
	OptLoggingLevel      = "log-level"
	OptMaxChunks         = "max-chunks"
	OptMaxConnPerHost    = "max-conn-per-host"
	OptMinimumChunkSize  = "minimum-chunk-size"
	OptOutputConsumer    = "output"
	OptResolve           = "resolve"
	OptRetries           = "retries"
	OptVerbose           = "verbose"
)

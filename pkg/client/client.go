package client

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/logging"
	"github.com/replicate/pget/pkg/optname"
	"github.com/replicate/pget/pkg/version"
)

const (
	// These are boundings for the retryablehttp client and not absolute values
	// see retryablehttp.LinearJitterBackoff for more details
	retryMinWait = 850 * time.Millisecond
	retryMaxWait = 1250 * time.Millisecond
)

// HTTPClient is a wrapper around http.Client that allows for limiting the number of concurrent connections per host
// utilizing a client pool. If the MaxConnPerHost option is not set, the client pool will not be used.
type HTTPClient struct {
	*http.Client
}

func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", fmt.Sprintf("pget/%s", version.GetVersion()))
	return c.Client.Do(req)
}

type HTTPClientOpts struct {
	ForceHTTP2     bool
	MaxConnPerHost int
	MaxRetries     int
}

// NewHTTPClient factory function returns a new http.Client with the appropriate settings and can limit number of clients
// per host if the MaxConnPerHost option is set.
func NewHTTPClient(opts HTTPClientOpts) *HTTPClient {
	disableKeepAlives := !opts.ForceHTTP2

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: transportDialContext(&net.Dialer{
			Timeout:   viper.GetDuration(optname.ConnTimeout),
			KeepAlive: 30 * time.Second,
		}),
		ForceAttemptHTTP2:     opts.ForceHTTP2,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     disableKeepAlives,
	}
	transport.MaxConnsPerHost = opts.MaxConnPerHost
	transport.MaxIdleConnsPerHost = opts.MaxConnPerHost

	retryClient := &retryablehttp.Client{
		HTTPClient: &http.Client{
			Transport:     transport,
			CheckRedirect: checkRedirectFunc,
		},
		Logger:       nil,
		RetryWaitMin: retryMinWait,
		RetryWaitMax: retryMaxWait,
		RetryMax:     opts.MaxRetries,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      linearJitterRetryAfterBackoff,
	}

	client := retryClient.StandardClient()
	return &HTTPClient{Client: client}
}

// linearJitterRetryAfterBackoff wraps retryablehttp.LinearJitterBackoff but also will adhere to Retry-After responses
func linearJitterRetryAfterBackoff(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration {
	var retryAfter time.Duration

	if shouldApplyRetryAfter(resp) {
		retryAfter = evaluateRetryAfter(resp)
	}

	if retryAfter > 0 {
		// If the Retry-After header is set, treat this as attempt 0 to get just the jitter
		jitter := max - min
		return retryablehttp.LinearJitterBackoff(retryAfter, retryAfter+jitter, 0, resp)
	}

	return retryAfter + retryablehttp.LinearJitterBackoff(min, max, attemptNum, resp)
}

func evaluateRetryAfter(resp *http.Response) time.Duration {
	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter != "" {
		return 0
	}

	duration, err := strconv.ParseInt(retryAfter, 10, 64)
	if err != nil {
		return 0
	}

	return time.Second * time.Duration(duration)
}

func shouldApplyRetryAfter(resp *http.Response) bool {
	return !viper.GetBool(optname.IgnoreRetryAfter) && resp != nil && resp.StatusCode == http.StatusTooManyRequests
}

// checkRedirectFunc is a wrapper around http.Client.CheckRedirect that allows for printing out redirects
func checkRedirectFunc(req *http.Request, via []*http.Request) error {
	logger := logging.GetLogger()

	logger.Trace().
		Str("redirect_url", req.URL.String()).
		Str("url", via[0].URL.String()).
		Int("status", req.Response.StatusCode).
		Msg("Redirect")
	return nil
}

// transportDialContext is a wrapper around net.Dialer that allows for overriding DNS lookups via the values passed to
// `--resolve` argument.
func transportDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	logger := logging.GetLogger()

	// Allow for overriding DNS lookups in the dialer without impacting Host and SSL resolution
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addrOverride := config.HostToIPResolutionMap[addr]; addrOverride != "" {
			logger.Debug().Str("addr", addr).Str("override", addrOverride).Msg("DNS Override")
			addr = addrOverride
		}
		return dialer.DialContext(ctx, network, addr)
	}
}

func GetSchemeHostKey(urlString string) (string, error) {
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		return "", err
	}
	return getSchemeHostKey(parsedURL), err
}

func getSchemeHostKey(url *url.URL) string {
	return fmt.Sprintf("%s://%s", url.Scheme, url.Host)
}

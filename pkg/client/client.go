package client

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/logging"
	"github.com/replicate/pget/pkg/optname"
	"github.com/replicate/pget/pkg/version"
)

const (
	retryMinWait     = 100 * time.Millisecond  // in milliseconds
	retryMaxWait     = 3000 * time.Millisecond // in milliseconds, do not backoff further than 3 seconds
	retrySleepJitter = 500                     // (will add 0-500 additional milliseconds), multiplied by time.Millisecond in backoffFunc

)

var logger = logging.Logger

// HTTPClient is a wrapper around http.Client that allows for limiting the number of concurrent connections per host
// utilizing a client pool. If the MaxConnPerHost option is not set, the client pool will not be used.
type HTTPClient struct {
	*http.Client
	host string
}

// Done releases the client. This is a simple utility function that should be called in a defer statement.
func (c *HTTPClient) Done() {
	if viper.GetInt(optname.MaxConnPerHost) > 0 {
		if err := releaseClient(c); err != nil {
			logger.Error().Err(err).Msg("Error releasing client")
		}
	}
}

type UserAgentTransport struct {
	Transport http.RoundTripper
}

func (t *UserAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", fmt.Sprintf("pget/%s", version.GetVersion()))
	return t.Transport.RoundTrip(req)
}

// newClient factory function returns a new http.Client with the appropriate settings and can limit number of clients
// per host if the MaxConnPerHost option is set.
func newClient(host string) *HTTPClient {
	baseTransport := http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: transportDialContext(&net.Dialer{
			Timeout:   viper.GetDuration(optname.ConnTimeout),
			KeepAlive: 30 * time.Second,
		}),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
	}
	maxConnPerHost := viper.GetInt(optname.MaxConnPerHost)
	if maxConnPerHost > 0 {
		baseTransport.MaxConnsPerHost = maxConnPerHost
	}

	transport := &UserAgentTransport{Transport: &baseTransport}

	retryClient := &retryablehttp.Client{
		HTTPClient: &http.Client{
			Transport:     transport,
			CheckRedirect: checkRedirectFunc,
		},
		Logger:       nil,
		RetryWaitMin: retryMinWait,
		RetryWaitMax: retryMaxWait,
		RetryMax:     viper.GetInt(optname.Retries),
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      backoffFunc,
	}

	client := retryClient.StandardClient()
	return &HTTPClient{Client: client, host: host}
}

// backoffFunc is a wrapper around retryablehttp.DefaultBackoff that allows for adding a random jitter to the backoff
// we utilize the jitter to avoid thundering herd issues since we are running with significant numbers of concurrent
// downloads.
func backoffFunc(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration {
	sleep := time.Duration(rand.Intn(retrySleepJitter)) * time.Millisecond
	sleep += retryablehttp.DefaultBackoff(min, max, attemptNum, resp)
	return sleep
}

// checkRedirectFunc is a wrapper around http.Client.CheckRedirect that allows for printing out redirects
func checkRedirectFunc(req *http.Request, via []*http.Request) error {
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
	return fmt.Sprintf("%s%s", parsedURL.Scheme, parsedURL.Host), err
}

package client

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/logging"
	"github.com/replicate/pget/pkg/optname"
	"github.com/replicate/pget/pkg/version"
)

const (
	retryDelayBaseline = 100 // in milliseconds
	retrySleepJitter   = 500 // in milliseconds (will add 0-500 additional milliseconds)

	retryMaxBackoffTime = 3000 // in milliseconds, do not backoff further than 3 seconds
	retryBackoffIncr    = 500  // in milliseconds, backoffFactor^retryNum * backoffIncr
	retryBackoffFactor  = 2    // Base for POW()
)

var logger = logging.Logger

// HTTPClient is a wrapper around http.Client that allows for limiting the number of concurrent connections per host
// utilizing a client pool. If the MaxConnPerHost option is not set, the client pool will not be used.
type HTTPClient struct {
	*http.Client
	host string
}

// Done releases the semaphore. This is a simple utility function that should be called in a defer statement.
func (c *HTTPClient) Done() {
	if viper.GetInt(optname.MaxConnPerHost) > 0 {
		releaseClient(c)
	}
}

// R8GetRetryingRoundTripper is a wrapper around http.Transport that allows for retrying failed requests
type R8GetRetryingRoundTripper struct {
	Transport *http.Transport
}

func (rt R8GetRetryingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", fmt.Sprintf("pget/%s", version.GetVersion()))
	retries := viper.GetInt(optname.Retries)
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			if viper.GetBool(optname.Verbose) {
				fmt.Printf("Retrying. Count: %d\n", attempt)
			}
			sleepJitter := time.Duration(rand.Intn(retrySleepJitter))
			sleepTime := time.Millisecond * (sleepJitter + retryDelayBaseline)

			// Exponential backoff
			// 2^retryNum * retryBackoffIncr (in milliseconds)
			backoffFactor := math.Pow(retryBackoffFactor, float64(attempt))
			backoffDuration := time.Duration(math.Min(backoffFactor*retryBackoffIncr, retryMaxBackoffTime))
			sleepTime += time.Millisecond * backoffDuration
			time.Sleep(sleepTime)
		}

		if attempt > 0 {
			req.Header.Set("Retry-Count", fmt.Sprintf("%d", attempt))
		}

		resp, err := rt.Transport.RoundTrip(req)
		if err != nil {
			return nil, fmt.Errorf("error making request: %w", err)
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("file not found: %s", req.URL.String())
		}
		if resp.StatusCode >= http.StatusBadRequest {
			if viper.GetBool(optname.Verbose) {
				fmt.Printf("Received Status '%s', retrying\n", resp.Status)
			}
			continue
		}
		// Success! Exit the loop
		return resp, nil
	}
	return nil, fmt.Errorf("failed to download %s after %d retries", req.URL.String(), retries)
}

// newClient factory function returns a new http.Client with the appropriate settings and can limit number of clients
// per host if the MaxConnPerHost option is set.
func newClient(host string) *HTTPClient {
	transport := &http.Transport{
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
	}
	transport.DisableKeepAlives = false
	maxConnPerHost := viper.GetInt(optname.MaxConnPerHost)
	if maxConnPerHost > 0 {
		transport.MaxConnsPerHost = maxConnPerHost
	}

	client := &http.Client{
		Transport:     &R8GetRetryingRoundTripper{Transport: transport},
		CheckRedirect: checkRedirectFunc,
	}
	return &HTTPClient{Client: client, host: host}
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

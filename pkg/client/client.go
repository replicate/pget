package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/spf13/viper"

	"github.com/hashicorp/go-retryablehttp"

	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/logging"
	"github.com/replicate/pget/pkg/version"
)

const (
	// These are boundings for the retryablehttp client and not absolute values
	// see retryablehttp.LinearJitterBackoff for more details
	retryMinWait = 850 * time.Millisecond
	retryMaxWait = 1250 * time.Millisecond
)

var ErrStrategyFallback = errors.New("fallback to next strategy")

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// PGetHTTPClient is a wrapper around http.Client that allows for limiting the number of concurrent connections per host
// utilizing a client pool. If the OptMaxConnPerHost option is not set, the client pool will not be used.
type PGetHTTPClient struct {
	*http.Client
	authHeader string
}

func (c *PGetHTTPClient) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", fmt.Sprintf("pget/%s", version.GetVersion()))
	if c.authHeader != "" {
		req.Header.Set("Authorization", c.authHeader)
	}
	return c.Client.Do(req)
}

type Options struct {
	MaxRetries    int
	Transport     http.RoundTripper
	TransportOpts TransportOptions
}

type TransportOptions struct {
	ForceHTTP2       bool
	ResolveOverrides map[string]string
	MaxConnPerHost   int
	ConnectTimeout   time.Duration
}

// NewHTTPClient factory function returns a new http.Client with the appropriate settings and can limit number of clients
// per host if the OptMaxConnPerHost option is set.
func NewHTTPClient(opts Options) HTTPClient {

	transport := opts.Transport

	if transport == nil {
		topts := opts.TransportOpts
		dialer := &transportDialer{
			DNSOverrideMap: topts.ResolveOverrides,
			Dialer: &net.Dialer{
				Timeout:   topts.ConnectTimeout,
				KeepAlive: 30 * time.Second,
			},
		}

		disableKeepAlives := topts.ForceHTTP2
		transport = &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			ForceAttemptHTTP2:     topts.ForceHTTP2,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableKeepAlives:     disableKeepAlives,
			MaxConnsPerHost:       topts.MaxConnPerHost,
			MaxIdleConnsPerHost:   topts.MaxConnPerHost,
		}
	}

	retryClient := &retryablehttp.Client{
		HTTPClient: &http.Client{
			Transport:     transport,
			CheckRedirect: checkRedirectFunc,
		},
		Logger:       nil,
		RetryWaitMin: retryMinWait,
		RetryWaitMax: retryMaxWait,
		RetryMax:     opts.MaxRetries,
		CheckRetry:   RetryPolicy,
		Backoff:      linearJitterRetryAfterBackoff,
	}

	client := retryClient.StandardClient()
	return &PGetHTTPClient{Client: client, authHeader: viper.GetString(config.OptAuthHeader)}
}

// RetryPolicy wraps retryablehttp.DefaultRetryPolicy and included additional logic:
// - checks for specific errors that indicate a fall-back to the next download strategy
// - checks for http.StatusBadGateway and http.StatusServiceUnavailable which also indicate a fall-back
func RetryPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	// do not retry on context.Canceled or context.DeadlineExceeded, this is a fast-fail even though
	// the retryablehttp.ErrorPropagatedRetryPolicy will also return false for these errors. We can avoid
	// extra processing logic in these cases every time
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	// While type assertions are not ideal, alternatives are limited to adding custom data in the request
	// or in the context. The context clearly isolates this data.
	consistentHashing, ok := ctx.Value(config.ConsistentHashingStrategyKey).(bool)
	if ok && consistentHashing {
		if fallbackError(err) {
			return false, ErrStrategyFallback
		}
		if err == nil && (resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusServiceUnavailable) {
			return false, ErrStrategyFallback
		}
	}

	// Wrap the standard retry policy
	return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
}

// fallbackError returns true if the error is an error we should fall back to the next strategy.
// fallback errors are not retryable errors that indicate fundamental problems with the cache-server
// or networking to the cache server. These errors include connection timeouts, connection refused, dns
// lookup errors, etc.
func fallbackError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	ok := errors.As(err, &netErr)
	if ok && netErr.Timeout() {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Op == "dial" || opErr.Op == "read" {
			return true
		}
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTimeout || dnsErr.IsNotFound
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}

	return false
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
	return resp != nil && resp.StatusCode == http.StatusTooManyRequests
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

type transportDialer struct {
	DNSOverrideMap map[string]string
	Dialer         *net.Dialer
}

func (d *transportDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	logger := logging.GetLogger()
	if addrOverride := d.DNSOverrideMap[addr]; addrOverride != "" {
		logger.Debug().Str("addr", addr).Str("override", addrOverride).Msg("DNS Override")
		addr = addrOverride
	}
	return d.Dialer.DialContext(ctx, network, addr)
}

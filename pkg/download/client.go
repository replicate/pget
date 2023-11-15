package download

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
			if viper.GetBool(optname.Verbose) {
				fmt.Printf("Received error '%s', retrying\n", err.Error())
			}
			continue
		}

		if resp.StatusCode >= 400 {
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

// newClient factory function returns a new http.Client with the appropriate settings
func newClient() *http.Client {

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: transportDialContext(&net.Dialer{
			Timeout:   viper.GetDuration(optname.ConnTimeout),
			KeepAlive: 30 * time.Second,
		}),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	transport.DisableKeepAlives = true

	return &http.Client{
		Transport:     &R8GetRetryingRoundTripper{Transport: transport},
		CheckRedirect: checkRedirectFunc,
	}
}

// checkRedirectFunc is a wrapper around http.Client.CheckRedirect that allows for printing out redirects
func checkRedirectFunc(req *http.Request, via []*http.Request) error {
	if viper.GetBool(optname.Verbose) {
		fmt.Printf("Received redirect '%d' to '%s'\n", req.Response.StatusCode, req.URL.String())
	}
	return nil
}

// transportDialContext is a wrapper around net.Dialer that allows for overriding DNS lookups via the values passed to
// `--resolve` argument.
func transportDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	// Allow for overriding DNS lookups in the dialer without impacting Host and SSL resolution
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addrOverride := config.HostToIPResolutionMap[addr]; addrOverride != "" {
			if viper.GetBool(optname.Verbose) {
				fmt.Printf("Overriding Resolution of '%s' to '%s'", dialer.LocalAddr.String(), addrOverride)
				addr = addrOverride
			}
		}
		return dialer.DialContext(ctx, network, addr)
	}
}

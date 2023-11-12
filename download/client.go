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

	"github.com/replicate/pget/config"
	"github.com/replicate/pget/version"
)

const (
	retryDelayBaseline = 100 // in milliseconds
	retrySleepJitter   = 500 // in milliseconds (will add 0-500 additional milliseconds)

	retryMaxBackoffTime = 3000 // in milliseconds, do not backoff further than 3 seconds
	retryBackoffIncr    = 500  // in milliseconds, backoffFactor^retryNum * backoffIncr
	retryBackoffFactor  = 2    // Base for POW()
)

type R8GetRoundTripper struct {
	Transport *http.Transport
}

func (rt R8GetRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", fmt.Sprintf("r8get/%s", version.GetVersion()))
	for attempt := 0; attempt <= viper.GetInt("retries"); attempt++ {
		if attempt > 0 {
			if viper.GetBool("verbose") {
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
			if viper.GetBool("verbose") {
				fmt.Printf("Error: %s\n", err.Error())
			}
			continue
		}
		if resp.StatusCode >= 400 {
			if viper.GetBool("verbose") {
				fmt.Printf("Error: %s\n", resp.Status)
			}
			continue
		}
	}
	return nil, fmt.Errorf("failed to download %s", req.URL.String())
}

func newClient() *http.Client {

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: resolveOverrideDialerContext(&net.Dialer{
			Timeout:   30 * time.Second,
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
		Transport:     transport,
		CheckRedirect: checkRedirectFunc,
	}
}

func resolveOverrideDialerContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	dialerContext := func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addrOverride := config.HostToIPResolutionMap[addr]; addrOverride != "" {
			if viper.GetBool("verbose") {
				fmt.Printf("Overriding %s with %s\n", addr, addrOverride)
			}
			addr = addrOverride
		}
		return dialer.DialContext(ctx, network, addr)
	}
	return dialerContext
}

func checkRedirectFunc(req *http.Request, via []*http.Request) error {
	if viper.GetBool("verbose") {
		fmt.Printf("Received redirect '%d' to '%s'\n", req.Response.StatusCode, req.URL.String())
	}
	return nil
}

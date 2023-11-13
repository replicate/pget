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

	"github.com/replicate/pget/optname"
)

const (
	retryDelayBaseline = 100 // in milliseconds
	retrySleepJitter   = 500 // in milliseconds (will add 0-500 additional milliseconds)

	retryMaxBackoffTime = 3000 // in milliseconds, do not backoff further than 3 seconds
	retryBackoffIncr    = 500  // in milliseconds, backoffFactor^retryNum * backoffIncr
	retryBackoffFactor  = 2    // Base for POW()
)

type R8GetRetryingRoundTripper struct {
	Transport *http.Transport
}

func (rt R8GetRetryingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
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
				fmt.Printf("Error: %s\n", err.Error())
			}
			continue
		}
		if resp.StatusCode >= 400 {
			if viper.GetBool(optname.Verbose) {
				fmt.Printf("Error: %s\n", resp.Status)
			}
			continue
		}
	}
	return nil, fmt.Errorf("failed to download %s after %d retries", req.URL.String(), retries)
}

func newClient() *http.Client {

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: transportDialContext(&net.Dialer{
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
		Transport:     &R8GetRetryingRoundTripper{Transport: transport},
		CheckRedirect: checkRedirectFunc,
	}
}

func checkRedirectFunc(req *http.Request, via []*http.Request) error {
	if viper.GetBool(optname.Verbose) {
		fmt.Printf("Received redirect '%d' to '%s'\n", req.Response.StatusCode, req.URL.String())
	}
	return nil
}

func transportDialContext(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return dialer.DialContext
}

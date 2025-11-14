package client_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/config"
)

func TestRetryPolicy(t *testing.T) {
	bgCtx := context.Background()
	chCtx := context.WithValue(bgCtx, config.ConsistentHashingStrategyKey, true)
	errContext, cancel := context.WithCancel(bgCtx)
	cancel()

	urlError := &url.Error{Err: fmt.Errorf("stopped after 15 redirects"), URL: "http://example.com"}

	tc := []struct {
		name           string
		ctx            context.Context
		resp           *http.Response
		err            error
		expectedResult bool
		expectedError  error
	}{
		{
			name:           "context error",
			ctx:            errContext,
			resp:           &http.Response{},
			err:            context.Canceled,
			expectedResult: false,
			expectedError:  context.Canceled,
		},
		{
			name:           "net.OpErr: dial",
			ctx:            chCtx,
			resp:           &http.Response{},
			err:            &net.OpError{Op: "dial"},
			expectedResult: false,
			expectedError:  client.ErrStrategyFallback,
		},
		{
			name:           "net.OpErr: read",
			ctx:            chCtx,
			resp:           &http.Response{},
			err:            &net.OpError{Op: "read"},
			expectedResult: false,
			expectedError:  client.ErrStrategyFallback,
		},
		{
			name:           "net.OpErr: write",
			ctx:            chCtx,
			resp:           &http.Response{},
			err:            &net.OpError{Op: "write"},
			expectedResult: true,
		},
		{
			name:           "net.DNSErr: Timeout",
			ctx:            chCtx,
			resp:           &http.Response{},
			err:            &net.DNSError{IsTimeout: true},
			expectedResult: false,
			expectedError:  client.ErrStrategyFallback,
		},
		{
			name:           "net.DNSErr: IsTemporary",
			ctx:            chCtx,
			resp:           &http.Response{},
			err:            &net.DNSError{IsTemporary: true},
			expectedResult: true,
		},
		{
			name:           "net.DNSErr: IsNotFound",
			ctx:            chCtx,
			resp:           &http.Response{},
			err:            &net.DNSError{IsNotFound: true},
			expectedResult: false,
			expectedError:  client.ErrStrategyFallback,
		},
		{
			name:           "net.ErrClosed",
			ctx:            chCtx,
			resp:           &http.Response{},
			err:            net.ErrClosed,
			expectedResult: false,
			expectedError:  client.ErrStrategyFallback,
		},
		{
			name:           "Unrecoverable error",
			ctx:            chCtx,
			resp:           &http.Response{},
			err:            urlError,
			expectedResult: false,
		},
		{
			name:           "Status Bad Gateway",
			ctx:            chCtx,
			resp:           &http.Response{StatusCode: http.StatusBadGateway},
			expectedResult: false,
			expectedError:  client.ErrStrategyFallback,
		},
		{
			name:           "Status OK",
			ctx:            chCtx,
			resp:           &http.Response{StatusCode: http.StatusOK},
			err:            urlError,
			expectedResult: false,
		},
		{
			name:           "Status Service Unavailable",
			ctx:            chCtx,
			resp:           &http.Response{StatusCode: http.StatusServiceUnavailable},
			expectedResult: false,
			expectedError:  client.ErrStrategyFallback,
		},
		{
			name:           "Recoverable Error",
			ctx:            chCtx,
			resp:           &http.Response{StatusCode: http.StatusOK},
			err:            fmt.Errorf("some error"),
			expectedResult: true,
		},
		{
			name:           "Too Many Requests",
			ctx:            chCtx,
			resp:           &http.Response{StatusCode: http.StatusTooManyRequests},
			expectedResult: true,
		},
		{
			name:           "Bad Gateway - no consistent-hash-context",
			ctx:            bgCtx,
			resp:           &http.Response{StatusCode: http.StatusBadGateway},
			expectedResult: true,
		},
		{
			name:           "net.OpErr: Dial - no consistent-hash-context",
			ctx:            bgCtx,
			resp:           &http.Response{},
			expectedResult: true,
			err:            &net.OpError{Op: "dial"},
		},
	}

	for _, tc := range tc {
		t.Run(tc.name, func(t *testing.T) {
			actualResult, actualError := client.RetryPolicy(tc.ctx, tc.resp, tc.err)
			assert.Equal(t, tc.expectedResult, actualResult)
			if tc.expectedError != nil {
				assert.Equal(t, tc.expectedError, actualError)
			} else {
				assert.NoError(t, actualError)
			}
		})
	}
}

func TestPGetHTTPClient_Headers(t *testing.T) {
	// Create a test server that echoes back the headers
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write back the custom headers as response headers for verification
		for key, values := range r.Header {
			for _, value := range values {
				w.Header().Add("Echo-"+key, value)
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Set up viper with custom headers
	viper.Set(config.OptHeaders, map[string]string{
		"Authorization":   "Bearer test-token",
		"X-Custom-Header": "custom-value",
	})
	defer viper.Reset()

	// Create client
	httpClient := client.NewHTTPClient(client.Options{
		MaxRetries: 0,
	})

	// Make a request
	req, err := http.NewRequest("GET", server.URL, nil)
	require.NoError(t, err)

	resp, err := httpClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Verify that our custom headers were sent
	assert.Equal(t, "Bearer test-token", resp.Header.Get("Echo-Authorization"))
	assert.Equal(t, "custom-value", resp.Header.Get("Echo-X-Custom-Header"))

	// Verify that User-Agent is set and contains "pget"
	userAgent := resp.Header.Get("Echo-User-Agent")
	assert.Contains(t, userAgent, "pget/")
}

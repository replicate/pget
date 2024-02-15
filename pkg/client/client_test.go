package client_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/client"
)

func TestGetSchemeHostKey(t *testing.T) {
	expected := "http://example.com"
	actual, err := client.GetSchemeHostKey("http://example.com/foo/bar;baz/quux?animal=giraffe")

	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestRetryPolicy(t *testing.T) {
	bgCtx := context.Background()
	chCtx := context.WithValue(bgCtx, client.ConsistentHashingStrategyKey, true)
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

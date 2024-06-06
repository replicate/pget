package download

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockHTTPClient struct {
	doFunc    func(req *http.Request) (*http.Response, error)
	callCount atomic.Int32
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	m.callCount.Add(1)
	return m.doFunc(req)
}

func TestResumeDownload(t *testing.T) {
	tests := []struct {
		name           string
		serverContent  string
		bytesReceived  int64
		initialRange   string
		expectedError  error
		expectedOutput []byte
		expectedCalls  int32
	}{
		{
			name:           "successful download",
			serverContent:  "Hello, world!",
			bytesReceived:  0,
			initialRange:   "bytes=0-12",
			expectedError:  nil,
			expectedOutput: []byte("Hello, world!"),
			expectedCalls:  1,
		},
		{
			name:           "partial download",
			serverContent:  "Hello, world!",
			bytesReceived:  3,
			initialRange:   "bytes=7-12",
			expectedError:  nil,
			expectedOutput: []byte("world!"),
			expectedCalls:  1,
		},
		{
			name:           "network error",
			serverContent:  "Hello, world!",
			bytesReceived:  0,
			initialRange:   "bytes=0-12",
			expectedError:  errors.New("network error"),
			expectedOutput: nil,
			expectedCalls:  1,
		},
		{
			name:           "multi-pass download",
			serverContent:  "12345678901234567890",
			bytesReceived:  3,
			initialRange:   "bytes=10-19",
			expectedError:  nil,
			expectedOutput: []byte("0123456789"),
			expectedCalls:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.ServeContent(w, r, "", time.Time{}, bytes.NewReader([]byte(tt.serverContent)))
			}))
			defer server.Close()

			req, err := http.NewRequest("GET", server.URL, nil)
			assert.NoError(t, err)

			// Set the initial Range header from the test case
			req.Header.Set("Range", tt.initialRange)

			buffer := make([]byte, len(tt.expectedOutput))
			copy(buffer, tt.expectedOutput[:tt.bytesReceived])
			mockClient := &mockHTTPClient{
				doFunc: func(req *http.Request) (*http.Response, error) {
					if tt.name == "network error" {
						return nil, errors.New("network error")
					}
					if tt.name == "multi-pass download" {
						switch req.Header.Get("Range") {
						case "bytes=15-19":
							return &http.Response{
								StatusCode: http.StatusPartialContent,
								Body:       io.NopCloser(bytes.NewReader([]byte("56789"))),
								Header:     http.Header{"Content-Range": []string{"bytes 15-20/21"}},
							}, nil
						case "bytes=13-19":
							return &http.Response{
								StatusCode: http.StatusPartialContent,
								Body:       io.NopCloser(bytes.NewReader([]byte("34"))),
								Header:     http.Header{"Content-Range": []string{"bytes 13-20/21"}},
							}, nil
						}
					}
					return http.DefaultClient.Do(req)
				},
			}

			totalBytesReceived, err := resumeDownload(req, buffer[tt.bytesReceived:], mockClient, tt.bytesReceived)
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(tt.expectedOutput), totalBytesReceived)
				assert.Equal(t, tt.expectedOutput, buffer[:len(tt.expectedOutput)])
			}
			assert.Equal(t, tt.expectedCalls, mockClient.callCount.Load(), "Unexpected number of HTTP client calls")
		})
	}
}

func TestUpdateRangeRequestHeader(t *testing.T) {
	tests := []struct {
		name          string
		initialRange  string
		receivedBytes int64
		expectedRange string
		expectedError error
	}{
		{
			name:          "valid range header",
			initialRange:  "bytes=0-10",
			receivedBytes: 5,
			expectedRange: "bytes=5-10",
			expectedError: nil,
		},
		{
			name:          "non-zero initial range",
			initialRange:  "bytes=7-12",
			receivedBytes: 3,
			expectedRange: "bytes=10-12",
			expectedError: nil,
		},
		{
			name:          "missing range header",
			initialRange:  "",
			receivedBytes: 5,
			expectedRange: "",
			expectedError: errMissingRangeHeader,
		},
		{
			name:          "malformed range header",
			initialRange:  "bytes=malformed",
			receivedBytes: 5,
			expectedRange: "",
			expectedError: errMalformedRangeHeader,
		},
		{
			name:          "receivedBytes exceeds range",
			initialRange:  "bytes=0-10",
			receivedBytes: 15,
			expectedRange: "",
			expectedError: errInvalidContentRange,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "http://example.com", nil)
			assert.NoError(t, err)
			req.Header.Set("Range", tt.initialRange)

			err = updateRangeRequestHeader(req, tt.receivedBytes)
			if tt.expectedError != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedRange, req.Header.Get("Range"))
			}
		})
	}
}

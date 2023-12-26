package config

import (
	"net/url"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetLogLevel(t *testing.T) {
	testCases := []struct {
		name     string
		logLevel string
	}{
		{"debug", "debug"},
		{"info", "info"},
		{"warn", "warn"},
		{"error", "error"},
		{"unknown", "info"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setLogLevel(tc.logLevel)
			assert.Equal(t, tc.logLevel, zerolog.GlobalLevel().String())
		})
	}
}

func TestResolveOverrides(t *testing.T) {
	testCases := []struct {
		name     string
		resolve  []string
		expected map[string]string
		err      bool
	}{
		{"empty", []string{}, nil, false},
		{"single", []string{"example.com:80:127.0.0.1"}, map[string]string{"example.com:80": "127.0.0.1:80"}, false},
		{"multiple", []string{"example.com:80:127.0.0.1", "example.com:443:127.0.0.1"}, map[string]string{"example.com:80": "127.0.0.1:80", "example.com:443": "127.0.0.1:443"}, false},
		{"invalid ip", []string{"example.com:80:InvalidIPAddr"}, nil, true},
		{"duplicate host different target", []string{"example.com:80:127.0.0.1", "example.com:80:127.0.0.2"}, nil, true},
		{"duplicate host same target", []string{"example.com:80:127.0.0.1", "example.com:80:127.0.0.1"}, map[string]string{"example.com:80": "127.0.0.1:80"}, false},
		{"invalid format", []string{"example.com:80"}, nil, true},
		{"invalid hostname format, is IP Addr", []string{"127.0.0.1:443:127.0.0.2"}, nil, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resolveOverrides, err := ResolveOverridesToMap(tc.resolve)
			assert.Equal(t, tc.err, err != nil)
			assert.Equal(t, tc.expected, resolveOverrides)
		})
	}
}

func helperUrlParse(t *testing.T, uris ...string) []*url.URL {
	t.Helper()
	var urls []*url.URL
	for _, uri := range uris {
		u, err := url.Parse(uri)
		require.NoError(t, err)
		urls = append(urls, u)
	}
	return urls
}

func TestCacheableURIPrefixes(t *testing.T) {
	defer func() {
		viper.Reset()
	}()
	testCases := []struct {
		name     string
		prefixes []string
		expected map[string][]*url.URL
	}{
		{
			name:     "empty",
			expected: map[string][]*url.URL{},
		},
		{
			name:     "single",
			prefixes: []string{"http://example.com"},
			expected: map[string][]*url.URL{
				"example.com": helperUrlParse(t, "http://example.com"),
			},
		},
		{
			name: "multiple", prefixes: []string{"http://example.com", "http://example.org"},
			expected: map[string][]*url.URL{
				"example.com": helperUrlParse(t, "http://example.com"),
				"example.org": helperUrlParse(t, "http://example.org"),
			},
		},
		{
			name:     "multiple same domain merged",
			prefixes: []string{"http://example.com/path", "http://example.com/other"},
			expected: map[string][]*url.URL{
				"example.com": helperUrlParse(t, "http://example.com/path", "http://example.com/other"),
			},
		},
		{
			name:     "invalid ignored",
			prefixes: []string{"http://example.com", "http://example.org", "invalid"},
			expected: map[string][]*url.URL{
				"example.com": helperUrlParse(t, "http://example.com"),
				"example.org": helperUrlParse(t, "http://example.org"),
			},
		},
		{
			name:     "single with path",
			prefixes: []string{"http://example.com/path"},
			expected: map[string][]*url.URL{
				"example.com": helperUrlParse(t, "http://example.com/path"),
			},
		},
		{
			name:     "multiple with path",
			prefixes: []string{"http://example.com/path", "http://example.org/path"},
			expected: map[string][]*url.URL{
				"example.com": helperUrlParse(t, "http://example.com/path"),
				"example.org": helperUrlParse(t, "http://example.org/path"),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			viper.Set(OptCacheURIPrefixes, strings.Join(tc.prefixes, " "))
			actual := CacheableURIPrefixes()
			assert.Equal(t, tc.expected, actual)
			viper.Reset()
		})
	}
}

func TestGetCacheSRV(t *testing.T) {
	defer func() {
		viper.Reset()
	}()
	testCases := []struct {
		name            string
		srvName         string
		hostIP          string
		srvNameByHostIP string
		expected        string
	}{
		{"empty", "", "", ``, ""},
		{"provided", "cache.srv.name.example", "", ``, "cache.srv.name.example"},
		{"looked up", "", "192.0.2.37", `{"192.0.2.0/24":"cache.srv.name.example"}`, "cache.srv.name.example"},
		{"both provided", "direct", "192.0.2.37", `{"192.0.2.0/24":"from-map"}`, "direct"},
		{"chooses correct value from map",
			"",
			"192.0.2.37",
			`{
                          "192.0.2.0/27":  "cache-1",
                          "192.0.2.32/27": "cache-2"
                        }`,
			"cache-2"},
		{"missing from map", "", "192.0.2.37", `{"192.0.2.0/30":"cache.srv.name.example"}`, ""},
		{"hostIP but no map", "", "192.0.2.37", ``, ""},
		{"invalid map", "", "192.0.2.37", `{`, ""},
		{"invalid CIDR", "", "192.0.2.37", `{"500.0.2.0/0":"cache.srv.name.example"}`, ""},
		{"valid + invalid CIDRs",
			"",
			"192.0.2.37",
			`{
                           "192.0.2.0/24": "cache-valid",
                           "500.0.2.0/30": "cache-invalid"
                         }`,
			"cache-valid"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			viper.Set(OptCacheNodesSRVName, tc.srvName)
			viper.Set(OptHostIP, tc.hostIP)
			viper.Set(OptCacheNodesSRVNameByHostCIDR, tc.srvNameByHostIP)
			actual := GetCacheSRV()
			assert.Equal(t, tc.expected, actual)
			viper.Reset()
		})
	}
}

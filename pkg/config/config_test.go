package config

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
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

func TestConvertResolveHostsToMap(t *testing.T) {
	defer func() {
		HostToIPResolutionMap = map[string]string{}
		viper.Reset()
	}()

	testCases := []struct {
		name     string
		resolve  []string
		expected map[string]string
		err      bool
	}{
		{"empty", []string{}, map[string]string{}, false},
		{"single", []string{"example.com:80:127.0.0.1"}, map[string]string{"example.com:80": "127.0.0.1:80"}, false},
		{"multiple", []string{"example.com:80:127.0.0.1", "example.com:443:127.0.0.1"}, map[string]string{"example.com:80": "127.0.0.1:80", "example.com:443": "127.0.0.1:443"}, false},
		{"invalid ip", []string{"example.com:80:InvalidIPAddr"}, map[string]string{}, true},
		{"duplicate host", []string{"example.com:80:127.0.0.1", "example.com:80:127.0.0.2"}, map[string]string{"example.com:80": "127.0.0.1:80"}, true},
		{"invalid format", []string{"example.com:80"}, map[string]string{}, true},
		{"invalid hostname format, is IP Addr", []string{"127.0.0.1:443:127.0.0.2"}, map[string]string{}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			HostToIPResolutionMap = map[string]string{}
			viper.Set(OptResolve, strings.Join(tc.resolve, " "))
			err := convertResolveHostsToMap()
			assert.Equal(t, tc.err, err != nil)
			assert.Equal(t, tc.expected, HostToIPResolutionMap)
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

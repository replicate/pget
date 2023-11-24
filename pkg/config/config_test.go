package config

import (
	"github.com/replicate/pget/pkg/optname"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"strings"
	"testing"

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
			viper.Set(optname.Resolve, strings.Join(tc.resolve, " "))
			err := convertResolveHostsToMap()
			assert.Equal(t, tc.err, err != nil)
			assert.Equal(t, tc.expected, HostToIPResolutionMap)
			viper.Reset()
		})
	}
}

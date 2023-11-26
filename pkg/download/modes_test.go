package download

import (
	"testing"

	"github.com/replicate/pget/pkg/client"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/optname"
)

func TestGetMode(t *testing.T) {
	clientOpts := client.HTTPClientOpts{
		MaxConnPerHost: viper.GetInt(optname.MaxConnPerHost),
		ForceHTTP2:     viper.GetBool(optname.ForceHTTP2),
		MaxRetries:     viper.GetInt(optname.Retries),
	}
	httpClient := client.NewHTTPClient(clientOpts)
	testCases := []struct {
		name     string
		modeName string
		mode     Mode
		err      bool
	}{
		{"Get BufferMode", BufferModeName, &BufferMode{}, false},
		{"Get ExtractTarMode", ExtractTarModeName, &ExtractTarMode{}, false},
		{"Get Unknown Mode", "invalid", nil, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mode, err := GetMode(tc.modeName, httpClient)
			assert.IsType(t, tc.mode, mode)
			assert.Equal(t, tc.err, err != nil)
		})
	}
}

func TestGetBufferMode(t *testing.T) {
	clientOpts := client.HTTPClientOpts{
		MaxConnPerHost: viper.GetInt(optname.MaxConnPerHost),
		ForceHTTP2:     viper.GetBool(optname.ForceHTTP2),
		MaxRetries:     viper.GetInt(optname.Retries),
	}
	httpClient := client.NewHTTPClient(clientOpts)

	assert.IsType(t, &BufferMode{}, getBufferMode(httpClient))
}

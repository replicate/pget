package download

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/optname"
)

func TestGetMode(t *testing.T) {
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
			mode, err := GetMode(tc.modeName)
			assert.IsType(t, tc.mode, mode)
			assert.Equal(t, tc.err, err != nil)
		})
	}
}

func TestGetBufferMode(t *testing.T) {
	config := getModeConfig()
	assert.IsType(t, &BufferMode{}, getBufferMode(config))
}

func TestGetModeConfig(t *testing.T) {
	config := getModeConfig()
	assert.Equal(t, viper.GetInt(optname.MaxConnPerHost), config.maxConnPerHost)
	assert.Equal(t, viper.GetBool(optname.ForceHTTP2), config.forceHTTP2)
}

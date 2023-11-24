package download

import (
	"fmt"
	"time"

	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/optname"
)

type modeFactory func(config ModeConfiguration) Mode

// ModeConfiguration is a struct that holds the configuration for a Mode
type ModeConfiguration struct {
	maxConnPerHost int
	forceHTTP2     bool
	maxRetries     int
}

type Mode interface {
	DownloadFile(url string, dest string) (fileSize int64, elapsedTime time.Duration, err error)
}

func modeFactories() map[string]modeFactory {
	return map[string]modeFactory{
		BufferModeName:     getBufferMode,
		ExtractTarModeName: getExtractTarMode,
	}
}

func GetMode(name string) (Mode, error) {
	factory, ok := modeFactories()[name]
	if !ok {
		return nil, fmt.Errorf("unknown mode: %s", name)
	}
	config := getModeConfig()
	return factory(config), nil
}

// getModeConfig returns a ModeConfiguration struct with the values from the viper config
// This should be the only function withing the modes that directly accesses viper
func getModeConfig() ModeConfiguration {
	return ModeConfiguration{
		maxConnPerHost: viper.GetInt(optname.MaxConnPerHost),
		forceHTTP2:     viper.GetBool(optname.ForceHTTP2),
		maxRetries:     viper.GetInt(optname.Retries),
	}
}

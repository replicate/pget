package download

import (
	"fmt"
	"time"

	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/optname"
)

type modeFactoryFunc func() Mode

type modeFactories struct {
	modes map[string]modeFactoryFunc
}

func (m *modeFactories) Get(name string) (Mode, error) {
	modeFactory, exists := m.modes[name]
	if !exists {
		return nil, fmt.Errorf("mode %s does not exist", name)
	}
	return modeFactory(), nil
}

func (m *modeFactories) GetModeNames() []string {
	var names []string
	for name := range m.modes {
		names = append(names, name)
	}
	return names
}

func getModes() *modeFactories {
	return &modeFactories{
		modes: map[string]modeFactoryFunc{
			BufferModeName: func() Mode {
				return &BufferMode{Client: client.NewClient(
					viper.GetBool(optname.ForceHTTP2),
					viper.GetInt(optname.MaxConnPerHost))}
			},
			TarExtractModeName: func() Mode { return &ExtractTarMode{} },
		},
	}
}

type Mode interface {
	DownloadFile(url string, dest string) (fileSize int64, elapsedTime time.Duration, err error)
}

func GetMode(name string) (Mode, error) {
	return getModes().Get(name)
}

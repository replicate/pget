package download

import (
	"fmt"
	"time"

	"github.com/replicate/pget/pkg/client"
)

type modeFactory func(client *client.HTTPClient) Mode

type Mode interface {
	DownloadFile(url string, dest string) (fileSize int64, elapsedTime time.Duration, err error)
}

func modeFactories() map[string]modeFactory {
	return map[string]modeFactory{
		BufferModeName:     getBufferMode,
		ExtractTarModeName: getExtractTarMode,
	}
}

func GetMode(name string, client *client.HTTPClient) (Mode, error) {
	factory, ok := modeFactories()[name]
	if !ok {
		return nil, fmt.Errorf("unknown mode: %s", name)
	}
	return factory(client), nil
}

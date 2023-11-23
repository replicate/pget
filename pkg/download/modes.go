package download

import (
	"fmt"
	"time"
)

type modeFactoryFunc func() Mode

var modes = map[string]modeFactoryFunc{
	"buffer":      func() Mode { return &BufferMode{} },
	"tar-extract": func() Mode { return &ExtractTarMode{} },
}

type Mode interface {
	DownloadFile(url string, dest string) (fileSize int64, elapsedTime time.Duration, err error)
}

func GetMode(name string) Mode {
	return modes[name]()
}

// AddMode registers a new mode with the given name, this is intended for use with testing only
// make sure to call the returned function to clean up after the test is done
func AddMode(name string, factory modeFactoryFunc) (func(), error) {
	if _, exists := modes[name]; exists {
		return nil, fmt.Errorf("mode %s already exists", name)
	}
	modes[name] = factory
	return func() { delete(modes, name) }, nil
}

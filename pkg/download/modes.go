package download

import "time"

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

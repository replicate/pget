package download

import "github.com/replicate/pget/pkg/client"

type modeFactoryFunc func() Mode

var modes = map[string]modeFactoryFunc{
	"buffer":      func() Mode { return &BufferMode{Client: client.NewClient()} },
	"tar-extract": func() Mode { return &ExtractTarMode{} },
}

type Mode interface {
	DownloadFile(url string, dest string) error
}

func GetMode(name string) Mode {
	return modes[name]()
}

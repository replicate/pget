package multifile

import (
	"io"

	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/consumer"
)

type MultiConsumer struct {
	consumerMap     map[string]consumer.Consumer
	defaultConsumer consumer.Consumer
}

var _ consumer.Consumer = &MultiConsumer{}

func (f MultiConsumer) Consume(reader io.Reader, destPath string, fileSize int64, contentType string) error {
	if c, ok := f.consumerMap[contentType]; ok {
		return c.Consume(reader, destPath, fileSize, contentType)
	}
	return f.defaultConsumer.Consume(reader, destPath, fileSize, contentType)
}

func (f MultiConsumer) EnableOverwrite() {
	f.defaultConsumer.EnableOverwrite()
	for _, c := range f.consumerMap {
		c.EnableOverwrite()
	}
}

func (f MultiConsumer) addConsumer(contentType, consumerName string) error {
	// TODO: Consider making this check content-type instead of just file extension
	c, err := config.GetConsumerByName(consumerName)
	if err != nil {
		return err
	}
	f.consumerMap[contentType] = c
	return nil
}

package consumer_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/consumer"
)

func TestNullWriter_Consume(t *testing.T) {
	buf := generateTestContent(1024)
	reader := bytes.NewReader(buf)

	nullConsumer := consumer.NullWriter{}
	err := nullConsumer.Consume(reader, "", 1024)
	assert.NoError(t, err)

	_, _ = reader.Seek(0, 0)
	err = nullConsumer.Consume(reader, "", 100)
	assert.Error(t, err)
}

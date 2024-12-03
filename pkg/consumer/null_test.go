package consumer_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/replicate/pget/pkg/consumer"
)

func TestNullWriter_Consume(t *testing.T) {
	r := require.New(t)
	buf := generateTestContent(kB)
	reader := bytes.NewReader(buf)

	nullConsumer := consumer.NullWriter{}
	r.NoError(nullConsumer.Consume(reader, "", kB))

	_, _ = reader.Seek(0, 0)
	r.Error(nullConsumer.Consume(reader, "", kB-100))
}

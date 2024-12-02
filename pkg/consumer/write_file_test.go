package consumer_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/consumer"
)

func TestFileWriter_Consume(t *testing.T) {

	buf := generateTestContent(1024)
	reader := bytes.NewReader(buf)

	writeFileConsumer := consumer.FileWriter{}
	tmpFile, _ := os.CreateTemp("", "fileWriterTest-")

	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	err := writeFileConsumer.Consume(reader, tmpFile.Name(), 1024)
	assert.NoError(t, err)

	// Check the file content is correct
	fileContent, _ := os.ReadFile(tmpFile.Name())
	assert.Equal(t, buf, fileContent)

	_, _ = reader.Seek(0, 0)
	err = writeFileConsumer.Consume(reader, "", 100)
	assert.Error(t, err)

	// test overwrite
	// overwrite the file
	f, err := os.OpenFile(tmpFile.Name(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	assert.NoError(t, err)
	_, _ = f.Write([]byte("different content"))
	f.Close()

	// consume the reader
	_, _ = reader.Seek(0, 0)
	writeFileConsumer.Overwrite = true
	err = writeFileConsumer.Consume(reader, tmpFile.Name(), 1024)
	assert.NoError(t, err)

	// check the file content is correct
	fileContent, _ = os.ReadFile(tmpFile.Name())
	assert.Equal(t, buf, fileContent)
}

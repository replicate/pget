package consumer_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/replicate/pget/pkg/consumer"
)

func TestFileWriter_Consume(t *testing.T) {
	r := require.New(t)

	buf := generateTestContent(kB)
	reader := bytes.NewReader(buf)

	writeFileConsumer := consumer.FileWriter{}
	tmpFile, _ := os.CreateTemp("", "fileWriterTest-")

	t.Cleanup(func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	})

	r.NoError(writeFileConsumer.Consume(reader, tmpFile.Name(), kB))

	// Check the file content is correct
	fileContent, _ := os.ReadFile(tmpFile.Name())
	r.Equal(buf, fileContent)

	_, _ = reader.Seek(0, 0)
	r.Error(writeFileConsumer.Consume(reader, "", kB-100))

	// test overwrite
	// overwrite the file
	f, err := os.OpenFile(tmpFile.Name(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	r.NoError(err)
	_, _ = f.Write([]byte("different content"))
	f.Close()

	// consume the reader
	_, _ = reader.Seek(0, 0)
	writeFileConsumer.Overwrite = true
	r.NoError(writeFileConsumer.Consume(reader, tmpFile.Name(), kB))

	// check the file content is correct
	fileContent, _ = os.ReadFile(tmpFile.Name())
	r.Equal(buf, fileContent)
}

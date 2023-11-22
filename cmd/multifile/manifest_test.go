package multifile

import (
	"bufio"
	"github.com/replicate/pget/pkg/client"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// validManifest is a valid manifest file with an additional empty line at the beginning
const validManifest = `
https://example.com/file1.txt /tmp/file1.txt
https://example.com/file2.txt /tmp/file2.txt
https://example.com/file3.txt /tmp/file3.txt`

const invalidManifest = `https://example.com/file1.txt`

func tempManifest(content string) (*os.File, error) {
	tempFile, err := os.CreateTemp("", "manifest")
	_, err = tempFile.WriteString(content)
	if err != nil {
		return nil, err
	}
	_, _ = tempFile.Seek(0, io.SeekStart)
	return tempFile, nil
}

func TestReadManifest(t *testing.T) {
	manifest, _ := tempManifest(validManifest)
	defer func() {
		manifest.Close()
		os.Remove(manifest.Name())
	}()

	scanner := bufio.NewScanner(manifest)
	buffer := readManifest(scanner)
	assert.Len(t, buffer, 3)
	// the first character is a \n, it should be stripped by readManifest
	assert.Equal(t, validManifest[1:], strings.Join(buffer, "\n"))
}

func TestParseLine(t *testing.T) {
	validLine := "https://example.com/file1.txt /tmp/file1.txt"
	invalidLine := "https://example.com/file1.txt"

	urlString, dest, err := parseLine(validLine)
	assert.Equal(t, "https://example.com/file1.txt", urlString)
	assert.Equal(t, "/tmp/file1.txt", dest)
	assert.NoError(t, err)

	urlString, dest, err = parseLine(invalidLine)
	assert.Error(t, err)
}

func TestCheckSeenDests(t *testing.T) {
	seenDests := map[string]string{
		"/tmp/file1.txt": "https://example.com/file1.txt",
	}

	err := checkSeenDests(seenDests, "/tmp/file1.txt", "https://example.com/file1.txt")
	assert.NoError(t, err)

	err = checkSeenDests(seenDests, "/tmp/file1.txt", "https://example.com/file2.txt")
	assert.Error(t, err)
}

func TestAddEntry(t *testing.T) {
	entries := make(map[string][]manifestEntry)
	schemeHostExample, _ := client.GetSchemeHostKey("https://example.com")
	schemHostExample2, _ := client.GetSchemeHostKey("https://example2.com")

	entries = addEntry(entries, schemeHostExample, "https://example.com/file1.txt", "/tmp/file1.txt")
	assert.Len(t, entries, 1)
	assert.Len(t, entries[schemeHostExample], 1)
	assert.Equal(t, "https://example.com/file1.txt", entries[schemeHostExample][0].url)
	assert.Equal(t, "/tmp/file1.txt", entries[schemeHostExample][0].dest)
	_, ok := entries[schemeHostExample]
	assert.True(t, ok)

	entries = addEntry(entries, schemeHostExample, "https://example.com/file2.txt", "/tmp/file2.txt")
	assert.Len(t, entries, 1)
	assert.Len(t, entries[schemeHostExample], 2)
	assert.Equal(t, "https://example.com/file2.txt", entries[schemeHostExample][1].url)
	assert.Equal(t, "/tmp/file2.txt", entries[schemeHostExample][1].dest)

	entries = addEntry(entries, schemHostExample2, "https://example2.com/file3.txt", "/tmp/file3.txt")
	assert.Len(t, entries, 2)
	assert.Len(t, entries[schemHostExample2], 1)
	assert.Equal(t, "https://example2.com/file3.txt", entries[schemHostExample2][0].url)
	assert.Equal(t, "/tmp/file3.txt", entries[schemHostExample2][0].dest)
	_, ok = entries[schemHostExample2]
	assert.True(t, ok)

}

func TestParseManifest(t *testing.T) {
	manifest := strings.Split(validManifest[1:], "\n")
	manifestMap, err := parseManifest(manifest)
	hostSchemeKey, _ := client.GetSchemeHostKey(manifest[0])
	assert.NoError(t, err)
	assert.Len(t, manifestMap, 1)
	assert.Len(t, manifestMap[hostSchemeKey], 3)

	manifest = strings.Split(invalidManifest[1:], "\n")

	manifestMap, err = parseManifest([]string{invalidManifest})
	assert.Error(t, err)
	assert.Len(t, manifestMap, 0)
}

func TestManifestFileToBuffer(t *testing.T) {
	splitManifest := strings.Split(validManifest[1:], "\n")
	manifest, _ := tempManifest(validManifest)
	defer func() {
		manifest.Close()
		os.Remove(manifest.Name())
	}()

	buffer, err := manifestFileToBuffer(manifest.Name())
	assert.NoError(t, err)
	assert.Len(t, buffer, 3)
	assert.Equal(t, splitManifest, buffer)
}

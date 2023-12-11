package multifile

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	pget "github.com/replicate/pget/pkg"
	"github.com/replicate/pget/pkg/client"
)

// validManifest is a valid manifest file with additional empty lines
const validManifest = `
https://example.com/file1.txt /tmp/file1.txt
https://example.com/file2.txt /tmp/file2.txt

https://example.com/file3.txt /tmp/file3.txt`

const invalidManifest = `https://example.com/file1.txt`

func TestParseLine(t *testing.T) {
	validLine := "https://example.com/file1.txt /tmp/file1.txt"
	validLineTabs := "https://example.com/file1.txt\t/tmp/file1.txt"
	validLineMultipleSpace := "https://example.com/file1.txt    /tmp/file1.txt"
	invalidLine := "https://example.com/file1.txt"

	urlString, dest, err := parseLine(validLine)
	assert.Equal(t, "https://example.com/file1.txt", urlString)
	assert.Equal(t, "/tmp/file1.txt", dest)
	assert.NoError(t, err)
	urlString, dest, err = parseLine(validLineTabs)
	assert.Equal(t, "https://example.com/file1.txt", urlString)
	assert.Equal(t, "/tmp/file1.txt", dest)
	assert.NoError(t, err)
	urlString, dest, err = parseLine(validLineMultipleSpace)
	assert.Equal(t, "https://example.com/file1.txt", urlString)
	assert.Equal(t, "/tmp/file1.txt", dest)
	assert.NoError(t, err)

	_, _, err = parseLine(invalidLine)
	assert.Error(t, err)
}

func TestCheckSeenDests(t *testing.T) {
	seenDests := map[string]string{
		"/tmp/file1.txt": "https://example.com/file1.txt",
	}

	// a different destination is fine
	err := checkSeenDests(seenDests, "/tmp/file2.txt", "https://example.com/file2.txt")
	assert.NoError(t, err)

	// the same destination with a different URL is not fine
	err = checkSeenDests(seenDests, "/tmp/file1.txt", "https://example.com/file2.txt")
	assert.Error(t, err)

	// the same destination with the same URL is also not fine
	err = checkSeenDests(seenDests, "/tmp/file1.txt", "https://example.com/file1.txt")
	assert.Error(t, err)
}

func TestAddEntry(t *testing.T) {
	entries := make(pget.Manifest)
	schemeHostExample, _ := client.GetSchemeHostKey("https://example.com")
	schemHostExample2, _ := client.GetSchemeHostKey("https://example2.com")

	entries = addEntry(entries, schemeHostExample, "https://example.com/file1.txt", "/tmp/file1.txt")
	assert.Len(t, entries, 1)
	assert.Len(t, entries[schemeHostExample], 1)
	assert.Equal(t, "https://example.com/file1.txt", entries[schemeHostExample][0].URL)
	assert.Equal(t, "/tmp/file1.txt", entries[schemeHostExample][0].Dest)
	_, ok := entries[schemeHostExample]
	assert.True(t, ok)

	entries = addEntry(entries, schemeHostExample, "https://example.com/file2.txt", "/tmp/file2.txt")
	assert.Len(t, entries, 1)
	assert.Len(t, entries[schemeHostExample], 2)
	assert.Equal(t, "https://example.com/file2.txt", entries[schemeHostExample][1].URL)
	assert.Equal(t, "/tmp/file2.txt", entries[schemeHostExample][1].Dest)

	entries = addEntry(entries, schemHostExample2, "https://example2.com/file3.txt", "/tmp/file3.txt")
	assert.Len(t, entries, 2)
	assert.Len(t, entries[schemHostExample2], 1)
	assert.Equal(t, "https://example2.com/file3.txt", entries[schemHostExample2][0].URL)
	assert.Equal(t, "/tmp/file3.txt", entries[schemHostExample2][0].Dest)
	_, ok = entries[schemHostExample2]
	assert.True(t, ok)

}

func TestParseManifest(t *testing.T) {
	manifest := strings.Split(validManifest[1:], "\n")
	manifestMap, err := parseManifest(strings.NewReader(validManifest))
	hostSchemeKey, _ := client.GetSchemeHostKey(manifest[0])
	assert.NoError(t, err)
	assert.Len(t, manifestMap, 1)
	assert.Len(t, manifestMap[hostSchemeKey], 3)

	manifestMap, err = parseManifest(strings.NewReader(invalidManifest))
	assert.Error(t, err)
	assert.Len(t, manifestMap, 0)
}

func TestManifestFile(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "manifest")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	file1, err := manifestFile("-")
	assert.NoError(t, err)
	assert.Equal(t, os.Stdin, file1)

	file2, err := manifestFile(tempFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, tempFile.Name(), file2.Name())

	_, err = manifestFile("/does/not/exist")
	assert.Error(t, err)
}

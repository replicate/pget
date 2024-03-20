package multifile

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestCheckSeenDestinations(t *testing.T) {
	seenDestinations := map[string]string{
		"/tmp/file1.txt": "https://example.com/file1.txt",
	}

	// a different destination is fine
	err := checkSeenDestinations(seenDestinations, "/tmp/file2.txt", "https://example.com/file2.txt")
	require.NoError(t, err)

	// the same destination with a different URL is not fine
	err = checkSeenDestinations(seenDestinations, "/tmp/file1.txt", "https://example.com/file2.txt")
	assert.Error(t, err)

	// the same destination with the same URL is fine, we raise a specific error to detect and skip
	err = checkSeenDestinations(seenDestinations, "/tmp/file1.txt", "https://example.com/file1.txt")
	assert.ErrorIs(t, err, errDupeURLDestCombo)
}

func TestParseManifest(t *testing.T) {
	parsedManifest, err := parseManifest(strings.NewReader(validManifest))
	assert.NoError(t, err)
	assert.Len(t, parsedManifest, 3)

	parsedManifest, err = parseManifest(strings.NewReader(invalidManifest))
	assert.Error(t, err)
	assert.Len(t, parsedManifest, 0)
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

func TestGetExtension(t *testing.T) {
	testCases := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "no_extension",
			path:     "/home/user/filename",
			expected: "",
		},
		{
			name:     "single_extension",
			path:     "/home/user/filename.txt",
			expected: ".txt",
		},
		{
			name:     "double_extension",
			path:     "/home/user/filename.txt.gz",
			expected: ".gz",
		},
		{
			name:     "hidden_file_with_extension",
			path:     "/home/user/.filename.txt",
			expected: ".txt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if res := getExtension(tc.path); res != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, res)
			}
		})
	}
}

package multifile

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/spf13/viper"

	pget "github.com/replicate/pget/pkg"
	"github.com/replicate/pget/pkg/cli"
	"github.com/replicate/pget/pkg/config"
)

// A manifest is a file consisting of pairs of URLs and paths:
//
// http://example.com/foo/bar.txt     foo/bar.txt
// http://example.com/foo/bar/baz.txt foo/bar/baz.txt
//
// A manifest may contain blank lines.
// The pairs are separated by arbitrary whitespace.
//
// When we parse a manifest, we group by URL base (ie scheme://hostname) so that
// all URLs that may share a connection are grouped.

func manifestFile(manifestPath string) (*os.File, error) {
	if manifestPath == "-" {
		return os.Stdin, nil
	}
	if _, err := os.Stat(manifestPath); errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("manifest file %s does not exist", manifestPath)
	}
	file, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("error opening manifest file %s: %w", manifestPath, err)
	}
	return file, err
}

func parseLine(line string) (urlString, dest string, err error) {
	fields := strings.Fields(line)
	if len(fields) != 2 {
		return "", "", fmt.Errorf("error parsing manifest invalid line format `%s`", line)
	}
	return fields[0], fields[1], nil
}

func checkSeenDestinations(destinations map[string]string, dest string, urlString string) error {
	if seenURL, ok := destinations[dest]; ok {
		if seenURL != urlString {
			return fmt.Errorf("duplicate destination %s with different urls: %s and %s", dest, seenURL, urlString)
		} else {
			return fmt.Errorf("duplicate entry: %s %s", urlString, dest)
		}
	}
	return nil
}

func parseManifest(file io.Reader) (pget.Manifest, error) {
	seenDestinations := make(map[string]string)
	manifest := make(pget.Manifest, 0)

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		urlString, dest, err := parseLine(line)
		if err != nil {
			return nil, err
		}

		// THIS IS A BODGE - FIX ME MOVE THESE THINGS TO PGET
		// and make the consumer responsible for knowing if this
		// is allowed/not allowed/etc
		consumer := viper.GetString(config.OptOutputConsumer)
		if consumer != config.ConsumerNull {
			err = checkSeenDestinations(seenDestinations, dest, urlString)
			if err != nil {
				return nil, err
			}
			seenDestinations[dest] = urlString

			err = cli.EnsureDestinationNotExist(dest)
			if err != nil {
				return nil, err
			}
		}

		manifest, err = manifest.AddEntry(urlString, dest)
		if err != nil {
			return nil, fmt.Errorf("error adding url: %w", err)
		}
	}

	return manifest, nil
}

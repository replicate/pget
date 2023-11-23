package multifile

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/cli"
	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/logging"
	"github.com/replicate/pget/pkg/optname"
)

func manifestFileToBuffer(manifestPath string) ([]string, error) {
	if manifestPath == "-" {
		return readManifest(bufio.NewScanner(os.Stdin)), nil
	}

	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("manifest file %s does not exist", manifestPath)
	}

	manifestFile, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("error opening manifest file %s: %s", manifestPath, err)
	}
	defer manifestFile.Close()

	return readManifest(bufio.NewScanner(manifestFile)), nil
}

// readManifest processes the manifest file into a slice of string
func readManifest(scanner *bufio.Scanner) []string {
	buffer := make([]string, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			buffer = append(buffer, line)
		}
	}
	return buffer
}

func parseLine(line string) (urlString, dest string, err error) {
	_, err = fmt.Sscanf(line, "%s %s", &urlString, &dest)
	if err != nil {
		err = fmt.Errorf("error parsing manifest invalid line format %s: %w", line, err)
	}
	return
}

func checkSeenDests(seenDests map[string]string, dest string, urlString string) error {
	if seenURL, ok := seenDests[dest]; ok {
		if seenURL != urlString {
			return fmt.Errorf("duplicate destination %s with different urls: %s and %s", dest, seenURL, urlString)
		}
	}
	return nil
}

func handleConnectionPoolCreate(schemeHost string) error {
	if viper.GetInt(optname.MaxConnPerHost) > 0 {
		client.CreateHostConnectionPool(schemeHost)
	}
	return nil
}

func addEntry(manifestMap map[string][]manifestEntry, schemeHost string, urlString string, dest string) map[string][]manifestEntry {
	logging.Logger.Debug().Str("url", urlString).Str("dest", dest).Msg("Queueing Download")

	entries, ok := manifestMap[schemeHost]

	if !ok {
		manifestMap[schemeHost] = []manifestEntry{{urlString, dest}}
	} else {
		manifestMap[schemeHost] = append(entries, manifestEntry{urlString, dest})
	}

	return manifestMap
}

func parseManifest(buffer []string) (map[string][]manifestEntry, error) {
	seenDests := make(map[string]string)
	manifestMap := make(map[string][]manifestEntry)

	if perHostLimit := viper.GetInt(optname.MaxConnPerHost); perHostLimit > 0 {
		logging.Logger.Debug().Int("max_connections_per_host", perHostLimit).Msg("Config")
	}

	for _, line := range buffer {
		urlString, dest, err := parseLine(line)
		if err != nil {
			return nil, err
		}

		err = checkSeenDests(seenDests, dest, urlString)
		if err != nil {
			return nil, err
		}

		seenDests[dest] = urlString

		err = cli.EnsureDestinationNotExist(dest)
		if err != nil {
			return nil, err
		}

		schemeHost, err := client.GetSchemeHostKey(urlString)
		if err != nil {
			return nil, fmt.Errorf("error parsing url %s: %w", urlString, err)
		}

		err = handleConnectionPoolCreate(schemeHost)
		if err != nil {
			return nil, err
		}

		manifestMap = addEntry(manifestMap, schemeHost, urlString, dest)
	}

	return manifestMap, nil
}

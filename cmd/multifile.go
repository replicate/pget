package cmd

import (
	"bufio"
	"fmt"
	"net/url"
	"os"

	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/download"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/optname"
)

const multiFileLongDesc = `
'multifile' mode for pget takes a manifest file as input (can use '-' for stdin) and downloads all files listed in the manifest.

The manifest is expected to be in the format of a newline-separated list of pairs of URLs and destination paths, separated by a space.
e.g.
https://example.com/file1.txt /tmp/file1.txt

'multifile'' will download files in parallel limited to the '--maximum-connections-per-host' limit for per-host limts and 
over-all limited to the '--max-concurrency' limit for overall concurrency.
`

// multifile mode config vars
var (
	MultifileMaxConnPerHost     int
	MultifileMaxConcurrentFiles int
)

var MultiFileCMD = &cobra.Command{
	Use:   "multifile [flags] <manifest-file>",
	Short: "download files from a manifest file in parallel",
	Long:  multiFileLongDesc,
	Args:  cobra.ExactArgs(1),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if viper.GetInt(optname.MaxConnPerHost) == 0 {
			viper.Set(optname.MaxConnPerHost, 40)
		}
		// Create the correct number of slots in the semaphore
		if viper.GetBool(optname.Extract) {
			return fmt.Errorf("cannot use --extract with multifile mode")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if err := execMultifile(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	RootCMD.AddCommand(MultiFileCMD)
	MultiFileCMD.PersistentFlags().IntVar(&MultifileMaxConnPerHost, optname.MaxConnPerHost, 0, "Maximum number of connections per host")
	MultiFileCMD.PersistentFlags().IntVar(&MultifileMaxConcurrentFiles, optname.MaxConcurrentFiles, 5, "Maximum number of files to download concurrently")
	err := viper.BindPFlags(MultiFileCMD.PersistentFlags())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type manifestEntry struct {
	url  string
	dest string
}

func execMultifile(cmd *cobra.Command, args []string) error {
	var reader *bufio.Reader
	// if manifest file is '-', read from stdin
	manifestPath := args[0]
	if manifestPath == "-" {
		reader = bufio.NewReader(os.Stdin)
	} else {
		// check that the manifest file exists
		_, err := os.Stat(manifestPath)
		if os.IsNotExist(err) {
			return fmt.Errorf("manifest file %s does not exist", manifestPath)
		}
		manifestFile, err := os.Open(manifestPath)
		if err != nil {
			return fmt.Errorf("error opening manifest file %s: %w", manifestPath, err)
		}
		defer manifestFile.Close()
		reader = bufio.NewReader(manifestFile)
	}
	// process the manifest file into a map of hosts to url/file pairs with processManifest
	buffer := make([]string, 0)
	for {
		readString, err := reader.ReadString('\n')
		// break on EOF or empty line
		if err != nil || readString == "\n" {
			break
		}
		buffer = append(buffer, readString)
	}
	manifest, err := processManifest(buffer)
	if err != nil {
		return fmt.Errorf("error processing manifest file %s: %w", manifestPath, err)
	}
	// download each host's files in parallel
	var eg errgroup.Group
	// If `--max-concurrent-files` is set, limit the number of concurrent files
	if concurrentFileLimit := viper.GetInt(optname.MaxConcurrentFiles); concurrentFileLimit > 0 {
		if viper.GetBool(optname.Verbose) {
			fmt.Printf("Downloading files with a maximum of %d concurrent files\n", concurrentFileLimit)
		}
		eg.SetLimit(concurrentFileLimit)
	}
	for host, entries := range manifest {
		err := downloadFilesFromHost(&eg, entries)
		if err != nil {
			return fmt.Errorf("error initiating download of files from host %s: %w", host, err)
		}
	}
	err = eg.Wait()
	if err != nil {
		return fmt.Errorf("error downloading files: %w", err)
	}
	return nil
}

func processManifest(buffer []string) (map[string][]manifestEntry, error) {
	// track the urls and dests for each host, do not allow for duplicate urls with different dests
	seenURLs := make(map[string]string)
	manifestMap := make(map[string][]manifestEntry)
	// read the manifest file line by line
	for _, line := range buffer {

		// split the line into url and dest
		var urlString, dest string
		_, err := fmt.Sscanf(line, "%s %s", &urlString, &dest)
		if err != nil {
			return nil, fmt.Errorf("error parsing manifest invalid line format %s: %w", line, err)
		}
		// check URL is not in seenURLs
		if seenDest, ok := seenURLs[urlString]; ok {
			if seenDest != dest {
				return nil, fmt.Errorf("duplicate url %s with different dests: %s and %s", urlString, seenDest, dest)
			}
		}
		// add the url to seenURLs
		seenURLs[urlString] = dest
		_, fileExists := os.Stat(dest)
		if !viper.GetBool(optname.Force) && !os.IsNotExist(fileExists) {
			return nil, fmt.Errorf("destination %s already exists", dest)

		}
		// get the host from the url
		parsedURL, err := url.Parse(urlString)
		if err != nil {
			return nil, fmt.Errorf("error parsing url %s: %w", urlString, err)
		}
		host := parsedURL.Host
		// add the url/dest pair to the manifestMap
		if entries, ok := manifestMap[host]; !ok {
			manifestMap[host] = []manifestEntry{{urlString, dest}}
		} else {
			manifestMap[host] = append(entries, manifestEntry{urlString, dest})
		}
	}
	return manifestMap, nil
}

func downloadFilesFromHost(eg *errgroup.Group, entries []manifestEntry) error {
	// Get the correct mode
	mode := download.GetMode(config.Mode)
	for _, entry := range entries {
		// Avoid 'capture by reference' issues by creating a new variable
		file := entry
		// acquire a slot in the semaphore
		eg.Go(func() error {
			return mode.DownloadFile(file.url, file.dest)
		})
	}
	return nil
}

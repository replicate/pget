package multifile

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/replicate/pget/pkg/cli"
	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/logging"
	"github.com/replicate/pget/pkg/optname"
)

const longDesc = `
'multifile' mode for pget takes a manifest file as input (can use '-' for stdin) and downloads all files listed in the manifest.

The manifest is expected to be in the format of a newline-separated list of pairs of URLs and destination paths, separated by a space.
e.g.
https://example.com/file1.txt /tmp/file1.txt

'multifile'' will download files in parallel limited to the '--maximum-connections-per-host' limit for per-host limts and 
over-all limited to the '--max-concurrency' limit for overall concurrency.
`

const multifileExamples = `
  pget multifile manifest.txt

  pget multifile - < manifest.txt

  cat multifile.txt | pget multifile -
`

// multifile mode config vars
var (
	maxConnPerHost     int
	maxConcurrentFiles int

	metricsMu       = &sync.Mutex{}
	downloadMetrics []multifileDownloadMetric
)

type manifestEntry struct {
	url  string
	dest string
}

type multifileDownloadMetric struct {
	elapsedTime time.Duration
	fileSize    int64
}

func GetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "multifile [flags] <manifest-file>",
		Short:   "download files from a manifest file in parallel",
		Long:    longDesc,
		Args:    cobra.ExactArgs(1),
		PreRunE: multifilePreRunE,
		RunE:    runMultifileCMD,
		Example: multifileExamples,
	}

	cmd.PersistentFlags().IntVar(&maxConnPerHost, optname.MaxConnPerHost, 0, "Maximum number of (global) concurrent connections per host (default 40)")
	cmd.PersistentFlags().IntVar(&maxConcurrentFiles, optname.MaxConcurrentFiles, 40, "Maximum number of files to download concurrently")
	err := viper.BindPFlags(cmd.PersistentFlags())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	cmd.SetUsageTemplate(cli.UsageTemplate)
	return cmd
}

func multifilePreRunE(cmd *cobra.Command, args []string) error {
	if viper.GetInt(optname.MaxConnPerHost) == 0 {
		viper.Set(optname.MaxConnPerHost, 40)
	}
	if viper.GetBool(optname.Extract) {
		return fmt.Errorf("cannot use --extract with multifile mode")
	}
	return nil
}

func runMultifileCMD(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	manifestPath := args[0]
	buffer, err := manifestFileToBuffer(manifestPath)
	if err != nil {
		return err
	}
	manifest, err := processManifest(buffer)
	if err != nil {
		return fmt.Errorf("error processing manifest file %s: %w", manifestPath, err)
	}

	return multifileExecute(manifest)
}

func initializeErrGroup() *errgroup.Group {
	var eg errgroup.Group

	// If `--max-concurrent-files` is set, limit the number of concurrent files
	if concurrentFileLimit := viper.GetInt(optname.MaxConcurrentFiles); concurrentFileLimit > 0 {
		logging.Logger.Debug().Int("concurrent_file_limit", concurrentFileLimit).Msg("Config")
		eg.SetLimit(concurrentFileLimit)
	}
	return &eg
}

func multifileExecute(manifest map[string][]manifestEntry) error {
	// download each host's files in parallel
	eg := initializeErrGroup()

	multifileDownloadStart := time.Now()

	for host, entries := range manifest {
		err := downloadFilesFromHost(eg, entries)
		if err != nil {
			return fmt.Errorf("error initiating download of files from host %s: %w", host, err)
		}
	}
	err := eg.Wait()
	if err != nil {
		return fmt.Errorf("error downloading files: %w", err)
	}

	aggregateAndPrintMetrics(time.Since(multifileDownloadStart))
	return nil
}

func aggregateAndPrintMetrics(elapsedTime time.Duration) {
	var totalFileSize int64

	metricsMu.Lock()
	defer metricsMu.Unlock()

	for _, metric := range downloadMetrics {
		totalFileSize += metric.fileSize

	}
	throughput := float64(totalFileSize) / elapsedTime.Seconds()
	logging.Logger.Info().
		Int("file_count", len(downloadMetrics)).
		Str("total_bytes_downloaded", humanize.Bytes(uint64(totalFileSize))).
		Str("throughput", fmt.Sprintf("%s/s", humanize.Bytes(uint64(throughput)))).
		Str("elapsed_time", fmt.Sprintf("%.3fs", elapsedTime.Seconds())).
		Msg("Metrics")
}

func downloadFilesFromHost(eg *errgroup.Group, entries []manifestEntry) error {
	// Get the correct mode
	mode := download.GetMode(config.Mode)

	for _, entry := range entries {
		// Avoid 'capture by reference' issues by creating a new variable
		file := entry
		eg.Go(func() error {
			return downloadAndMeasure(mode, file)
		})
	}
	return nil
}

func downloadAndMeasure(mode download.Mode, entry manifestEntry) error {
	fileSize, elapsedTime, err := mode.DownloadFile(entry.url, entry.dest)
	if err != nil {
		return err
	}
	addDownloadMetrics(elapsedTime, fileSize)
	return nil
}

func addDownloadMetrics(elapsedTime time.Duration, fileSize int64) {
	result := multifileDownloadMetric{
		elapsedTime: elapsedTime,
		fileSize:    fileSize,
	}
	metricsMu.Lock()
	defer metricsMu.Unlock()
	downloadMetrics = append(downloadMetrics, result)
}

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

	logger = logging.GetLogger()
)

type manifestEntry struct {
	url  string
	dest string
}

type multifileDownloadMetric struct {
	elapsedTime time.Duration
	fileSize    int64
}

type downhloadMetrics struct {
	metrics []multifileDownloadMetric
	mut     sync.Mutex
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

	cmd.PersistentFlags().IntVar(&maxConnPerHost, optname.MaxConnPerHost, 40, "Maximum number of (global) concurrent connections per host (default 40)")
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
	if viper.GetBool(optname.Extract) {
		return fmt.Errorf("cannot use --extract with multifile mode")
	}
	return nil
}

func runMultifileCMD(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	manifestPath := args[0]
	file, err := manifestFile(manifestPath)
	if err != nil {
		return err
	}
	defer file.Close()
	manifest, err := parseManifest(file)
	if err != nil {
		return fmt.Errorf("error processing manifest file %s: %w", manifestPath, err)
	}

	return multifileExecute(manifest)
}

func initializeErrGroup() *errgroup.Group {
	var eg errgroup.Group

	// If `--max-concurrent-files` is set, limit the number of concurrent files
	if concurrentFileLimit := viper.GetInt(optname.MaxConcurrentFiles); concurrentFileLimit > 0 {
		logger.Debug().Int("concurrent_file_limit", concurrentFileLimit).Msg("Config")
		eg.SetLimit(concurrentFileLimit)
	}
	return &eg
}

func multifileExecute(manifest manifest) error {
	mode, err := download.GetMode(download.BufferModeName)
	if err != nil {
		return fmt.Errorf("error getting mode: %w", err)
	}
	if perHostLimit := viper.GetInt(optname.MaxConnPerHost); perHostLimit > 0 {
		logger.Debug().Int("max_connections_per_host", perHostLimit).Msg("Config")
	}

	metrics := &downhloadMetrics{
		metrics: make([]multifileDownloadMetric, 0),
		mut:     sync.Mutex{},
	}

	// download each host's files in parallel
	eg := initializeErrGroup()

	multifileDownloadStart := time.Now()

	for host, entries := range manifest {
		err := downloadFilesFromHost(mode, eg, entries, metrics)
		if err != nil {
			return fmt.Errorf("error initiating download of files from host %s: %w", host, err)
		}
	}
	err = eg.Wait()
	if err != nil {
		return fmt.Errorf("error downloading files: %w", err)
	}

	aggregateAndPrintMetrics(time.Since(multifileDownloadStart), metrics)
	return nil
}

func aggregateAndPrintMetrics(elapsedTime time.Duration, metrics *downhloadMetrics) {
	var totalFileSize int64

	metrics.mut.Lock()
	defer metrics.mut.Unlock()

	for _, metric := range metrics.metrics {
		totalFileSize += metric.fileSize

	}
	throughput := float64(totalFileSize) / elapsedTime.Seconds()
	logger.Info().
		Int("file_count", len(metrics.metrics)).
		Str("total_bytes_downloaded", humanize.Bytes(uint64(totalFileSize))).
		Str("throughput", fmt.Sprintf("%s/s", humanize.Bytes(uint64(throughput)))).
		Str("elapsed_time", fmt.Sprintf("%.3fs", elapsedTime.Seconds())).
		Msg("Metrics")
}

func downloadFilesFromHost(mode download.Mode, eg *errgroup.Group, entries []manifestEntry, metrics *downhloadMetrics) error {
	for _, entry := range entries {
		// Avoid the `entry` loop variable being captured by the
		// goroutine by creating new variables
		url, dest := entry.url, entry.dest
		logger.Debug().Str("url", url).Str("dest", dest).Msg("Queueing Download")

		eg.Go(func() error {
			return downloadAndMeasure(mode, url, dest, metrics)
		})
	}
	return nil
}

func downloadAndMeasure(mode download.Mode, url, dest string, metrics *downhloadMetrics) error {
	fileSize, elapsedTime, err := mode.DownloadFile(url, dest)
	if err != nil {
		return err
	}
	addDownloadMetrics(elapsedTime, fileSize, metrics)
	return nil
}

func addDownloadMetrics(elapsedTime time.Duration, fileSize int64, metrics *downhloadMetrics) {
	result := multifileDownloadMetric{
		elapsedTime: elapsedTime,
		fileSize:    fileSize,
	}
	metrics.mut.Lock()
	defer metrics.mut.Unlock()
	metrics.metrics = append(metrics.metrics, result)
}

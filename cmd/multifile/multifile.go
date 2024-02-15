package multifile

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	pget "github.com/replicate/pget/pkg"
	"github.com/replicate/pget/pkg/cli"
	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/logging"
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

// test seam
type Getter interface {
	DownloadFile(ctx context.Context, url string, dest string) (int64, time.Duration, error)
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

	err := viper.BindPFlags(cmd.PersistentFlags())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	cmd.SetUsageTemplate(cli.UsageTemplate)
	return cmd
}

func multifilePreRunE(cmd *cobra.Command, args []string) error {
	if viper.GetBool(config.OptExtract) {
		return fmt.Errorf("cannot use --extract with multifile mode")
	}
	if viper.GetString(config.OptOutputConsumer) == config.ConsumerTarExtractor {
		return fmt.Errorf("cannot use --output-consumer tar-extractor with multifile mode")
	}
	if viper.GetString(config.OptOutputConsumer) == config.ConsumerZipExtractor {
		return fmt.Errorf("cannot use --output-consumer zip-extractor with multifile mode")
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

	return multifileExecute(cmd.Context(), manifest)
}

func maxConcurrentFiles() int {
	maxConcurrentFiles := viper.GetInt(config.OptMaxConcurrentFiles)
	if maxConcurrentFiles == 0 {
		maxConcurrentFiles = 20
	}
	return maxConcurrentFiles
}

func multifileExecute(ctx context.Context, manifest pget.Manifest) error {
	minChunkSize, err := humanize.ParseBytes(viper.GetString(config.OptMinimumChunkSize))
	if err != nil {
		return err
	}

	// Get the resolution overrides
	resolveOverrides, err := config.ResolveOverridesToMap(viper.GetStringSlice(config.OptResolve))
	if err != nil {
		return fmt.Errorf("error parsing resolve overrides: %w", err)
	}

	clientOpts := client.Options{
		MaxRetries: viper.GetInt(config.OptRetries),
		TransportOpts: client.TransportOptions{
			ForceHTTP2:       viper.GetBool(config.OptForceHTTP2),
			ConnectTimeout:   viper.GetDuration(config.OptConnTimeout),
			MaxConnPerHost:   viper.GetInt(config.OptMaxConnPerHost),
			ResolveOverrides: resolveOverrides,
		},
	}
	downloadOpts := download.Options{
		MaxConcurrency: viper.GetInt(config.OptConcurrency),
		MinChunkSize:   int64(minChunkSize),
		Client:         clientOpts,
	}
	pgetOpts := pget.Options{
		MaxConcurrentFiles: maxConcurrentFiles(),
	}

	consumer, err := config.GetConsumer()
	if err != nil {
		return fmt.Errorf("error getting consumer: %w", err)
	}

	getter := &pget.Getter{
		Downloader: download.GetBufferMode(downloadOpts),
		Consumer:   consumer,
		Options:    pgetOpts,
	}

	// TODO DRY this
	if srvName := config.GetCacheSRV(); srvName != "" {
		downloadOpts.SliceSize = 500 * humanize.MiByte
		downloadOpts.CacheableURIPrefixes = config.CacheableURIPrefixes()
		downloadOpts.CacheUsePathProxy = viper.GetBool(config.OptCacheUsePathProxy)
		downloadOpts.CacheHosts, err = cli.LookupCacheHosts(srvName)
		if err != nil {
			return err
		}
		getter.Downloader, err = download.GetConsistentHashingMode(downloadOpts)
		if err != nil {
			return err
		}
	}

	totalFileSize, elapsedTime, err := getter.DownloadFiles(ctx, manifest)
	if err != nil {
		return err
	}

	throughput := float64(totalFileSize) / elapsedTime.Seconds()
	logger := logging.GetLogger()
	logger.Info().
		Int("file_count", numEntries(manifest)).
		Str("total_bytes_downloaded", humanize.Bytes(uint64(totalFileSize))).
		Str("throughput", fmt.Sprintf("%s/s", humanize.Bytes(uint64(throughput)))).
		Str("elapsed_time", fmt.Sprintf("%.3fs", elapsedTime.Seconds())).
		Msg("Metrics")

	return nil
}

func numEntries(manifest pget.Manifest) (totalEntries int) {
	for _, entries := range manifest {
		totalEntries += len(entries)
	}
	return
}

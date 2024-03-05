package root

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/replicate/pget/cmd/version"
	pget "github.com/replicate/pget/pkg"
	"github.com/replicate/pget/pkg/cli"
	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/logging"
)

const rootLongDesc = `
pget

PGet is a high performance, concurrent file downloader built in Go. It is designed to speed up and optimize file
downloads from cloud storage services such as Amazon S3 and Google Cloud Storage.

The primary advantage of PGet is its ability to download files in parallel using multiple threads. By dividing the file
into chunks and downloading multiple chunks simultaneously, PGet significantly reduces the total download time for large
files.

If the downloaded file is a tar archive, PGet can automatically extract the contents of the archive in memory, thus
removing the need for an additional extraction step.

The efficiency of PGet's tar extraction lies in its approach to handling data. Instead of writing the downloaded tar
file to disk and then reading it back into memory for extraction, PGet conducts the extraction directly from the
in-memory download buffer. This method avoids unnecessary memory copies and disk I/O, leading to an increase in
performance, especially when dealing with large tar files. This makes PGet not just a parallel downloader, but also an
efficient file extractor, providing a streamlined solution for fetching and unpacking files.
`

var concurrency int
var pidFile *cli.PIDFile
var chunkSize string

const chunkSizeDefault = "125M"

func GetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "pget [flags] <url> <dest>",
		Short:              "pget",
		Long:               rootLongDesc,
		PersistentPreRunE:  rootPersistentPreRunEFunc,
		PersistentPostRunE: rootPersistentPostRunEFunc,
		RunE:               runRootCMD,
		Args:               validateArgs,
		Example:            `  pget https://example.com/file.tar ./target-dir`,
	}
	cmd.Flags().BoolP(config.OptExtract, "x", false, "OptExtract archive after download")
	cmd.SetUsageTemplate(cli.UsageTemplate)
	config.ViperInit()
	if err := persistentFlags(cmd); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err := viper.BindPFlags(cmd.PersistentFlags())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = viper.BindPFlags(cmd.Flags())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return cmd
}

// defaultPidFilePath returns the default path for the PID file. Notably modern OS X variants
// have permissions difficulties in /var/run etc.
func defaultPidFilePath() string {
	// If we're on OS X, use the user's home directory
	// Otherwise, use /run
	path := "/run/pget.pid"
	if runtime.GOOS == "darwin" {
		path = os.Getenv("HOME") + "/.pget.pid"
	}
	return path
}

func pidFlock(pidFilePath string) error {
	pid, err := cli.NewPIDFile(pidFilePath)
	if err != nil {
		return err
	}
	err = pid.Acquire()
	if err != nil {
		return err
	}
	pidFile = pid
	return nil
}

func rootPersistentPreRunEFunc(cmd *cobra.Command, args []string) error {
	logger := logging.GetLogger()
	if err := config.PersistentStartupProcessFlags(); err != nil {
		return err
	}
	if cmd.CalledAs() != version.VersionCMDName {
		if err := pidFlock(viper.GetString(config.OptPIDFile)); err != nil {
			return err
		}
	}

	// Handle chunk size flags (deprecation and overwriting where needed)
	//
	// Expected Behavior for chunk size flags:
	// * If either cli option is set, use that value
	// * If both are set, emit an error
	// * If neither are set, use ENV values
	// ** If PGET_CHUNK_SIZE is set, use that value
	// ** If PGET_CHUNK_SIZE is not set, use PGET_MINIMUM_CHUNK_SIZE if set
	//    NOTE: PGET_MINIMUM_CHUNK_SIZE value is just set over the key for PGET_CHUNK_SIZE
	//    Warning message will be emitted
	// ** If both PGET_CHUNK_SIZE and PGET_MINIMUM_CHUNK_SIZE are set, use PGET_CHUNK_SIZE
	//    Warning message will be emitted
	// * If neither are set, use the default value

	changedMin := cmd.PersistentFlags().Changed(config.OptMinimumChunkSize)
	changedChunk := cmd.PersistentFlags().Changed(config.OptChunkSize)
	if changedMin && changedChunk {
		return fmt.Errorf("--minimum-chunk-size and --chunk-size cannot be used at the same time, use --chunk-size instead")
	} else if !(changedMin && changedChunk) {
		minChunkSizeEnv := viper.GetString(config.OptMinimumChunkSize)
		chunkSizeEnv := viper.GetString(config.OptChunkSize)
		if minChunkSizeEnv != chunkSizeDefault {
			if chunkSizeEnv == chunkSizeDefault {
				logger.Warn().Msg("Using PGET_MINIMUM_CHUNK_SIZE is deprecated, use PGET_CHUNK_SIZE instead")
				viper.Set(config.OptChunkSize, minChunkSizeEnv)
			} else {
				logger.Warn().Msg("Both PGET_MINIMUM_CHUNK_SIZE and PGET_CHUNK_SIZE are set, using PGET_CHUNK_SIZE")
			}
		}
	}

	return nil
}

func rootPersistentPostRunEFunc(cmd *cobra.Command, args []string) error {
	if pidFile != nil {
		return pidFile.Release()
	}
	return nil
}

func persistentFlags(cmd *cobra.Command) error {
	// Persistent Flags (applies to all commands/subcommands)
	cmd.PersistentFlags().IntVarP(&concurrency, config.OptConcurrency, "c", runtime.GOMAXPROCS(0)*4, "Maximum number of concurrent downloads/maximum number of chunks for a given file")
	cmd.PersistentFlags().IntVar(&concurrency, config.OptMaxChunks, runtime.GOMAXPROCS(0)*4, "Maximum number of chunks for a given file")
	cmd.PersistentFlags().Duration(config.OptConnTimeout, 5*time.Second, "Timeout for establishing a connection, format is <number><unit>, e.g. 10s")
	cmd.PersistentFlags().StringVarP(&chunkSize, config.OptChunkSize, "m", chunkSizeDefault, "Chunk size (in bytes) to use when downloading a file (e.g. 10M)")
	cmd.PersistentFlags().StringVar(&chunkSize, config.OptMinimumChunkSize, chunkSizeDefault, "Minimum chunk size (in bytes) to use when downloading a file (e.g. 10M)")
	cmd.PersistentFlags().BoolP(config.OptForce, "f", false, "OptForce download, overwriting existing file")
	cmd.PersistentFlags().StringSlice(config.OptResolve, []string{}, "OptResolve hostnames to specific IPs")
	cmd.PersistentFlags().IntP(config.OptRetries, "r", 5, "Number of retries when attempting to retrieve a file")
	cmd.PersistentFlags().BoolP(config.OptVerbose, "v", false, "OptVerbose mode (equivalent to --log-level debug)")
	cmd.PersistentFlags().String(config.OptLoggingLevel, "info", "Log level (debug, info, warn, error)")
	cmd.PersistentFlags().Bool(config.OptForceHTTP2, false, "OptForce HTTP/2")
	cmd.PersistentFlags().Int(config.OptMaxConnPerHost, 40, "Maximum number of (global) concurrent connections per host")
	cmd.PersistentFlags().StringP(config.OptOutputConsumer, "o", "file", "Output Consumer (file, tar, null)")
	cmd.PersistentFlags().String(config.OptPIDFile, defaultPidFilePath(), "PID file path")

	if err := hideAndDeprecateFlags(cmd); err != nil {
		return err
	}

	return nil
}

func hideAndDeprecateFlags(cmd *cobra.Command) error {
	// Hide flags from help, these are intended to be used for testing/internal benchmarking/debugging only
	if err := config.HideFlags(cmd, config.OptForceHTTP2, config.OptMaxConnPerHost, config.OptOutputConsumer); err != nil {
		return err
	}

	// DeprecatedFlag flags
	err := config.DeprecateFlags(cmd,
		config.DeprecatedFlag{Flag: config.OptMaxChunks, Msg: fmt.Sprintf("use --%s instead", config.OptConcurrency)},
		config.DeprecatedFlag{Flag: config.OptMinimumChunkSize, Msg: fmt.Sprintf("use --%s instead", config.OptChunkSize)},
	)
	if err != nil {
		return err
	}
	return nil

}

func runRootCMD(cmd *cobra.Command, args []string) error {
	// After we run through the PreRun functions we want to silence usage from being printed
	// on all errors
	cmd.SilenceUsage = true

	urlString := args[0]
	dest := args[1]

	log.Info().Str("url", urlString).
		Str("dest", dest).
		Str("chunk_size", viper.GetString(config.OptChunkSize)).
		Msg("Initiating")

	// OMG BODGE FIX THIS
	consumer := viper.GetString(config.OptOutputConsumer)
	if consumer != config.ConsumerNull {
		if err := cli.EnsureDestinationNotExist(dest); err != nil {
			return err
		}
	}
	if err := rootExecute(cmd.Context(), urlString, dest); err != nil {
		return err
	}

	return nil
}

// rootExecute is the main function of the program and encapsulates the general logic
// returns any/all errors to the caller.
func rootExecute(ctx context.Context, urlString, dest string) error {
	chunkSize, err := humanize.ParseBytes(viper.GetString(config.OptChunkSize))
	if err != nil {
		return fmt.Errorf("error parsing chunk size: %w", err)
	}

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
		ChunkSize:      int64(chunkSize),
		Client:         clientOpts,
	}

	consumer, err := config.GetConsumer()
	if err != nil {
		return err
	}

	getter := pget.Getter{
		Downloader: download.GetBufferMode(downloadOpts),
		Consumer:   consumer,
	}

	if viper.GetBool(config.OptExtract) {
		// TODO: decide what to do when --output is set *and* --extract is set
		log.Debug().Msg("Tar Extract Enabled")
		viper.Set(config.OptOutputConsumer, config.ConsumerTarExtractor)
	}

	// TODO DRY this
	if srvName := config.GetCacheSRV(); srvName != "" {
		downloadOpts.SliceSize = 500 * humanize.MiByte
		// FIXME: make this a config option
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

	_, _, err = getter.DownloadFile(ctx, urlString, dest)
	return err
}

func validateArgs(cmd *cobra.Command, args []string) error {
	if viper.GetString(config.OptOutputConsumer) == config.ConsumerNull {
		return cobra.RangeArgs(1, 2)(cmd, args)
	}
	return cobra.ExactArgs(2)(cmd, args)
}

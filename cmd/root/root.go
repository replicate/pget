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

func GetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "pget [flags] <url> <dest>",
		Short:              "pget",
		Long:               rootLongDesc,
		PersistentPreRunE:  rootPersistentPreRunEFunc,
		PersistentPostRunE: rootPersistentPostRunEFunc,
		PreRun:             rootCmdPreRun,
		RunE:               runRootCMD,
		Args:               cobra.ExactArgs(2),
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
	if err := config.PersistentStartupProcessFlags(); err != nil {
		return err
	}
	if cmd.CalledAs() != version.VersionCMDName {
		if err := pidFlock(viper.GetString(config.OptPIDFile)); err != nil {
			return err
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
	cmd.PersistentFlags().StringP(config.OptMinimumChunkSize, "m", "16M", "Minimum chunk size (in bytes) to use when downloading a file (e.g. 10M)")
	cmd.PersistentFlags().BoolP(config.OptForce, "f", false, "OptForce download, overwriting existing file")
	cmd.PersistentFlags().StringSlice(config.OptResolve, []string{}, "OptResolve hostnames to specific IPs")
	cmd.PersistentFlags().IntP(config.OptRetries, "r", 5, "Number of retries when attempting to retrieve a file")
	cmd.PersistentFlags().BoolP(config.OptVerbose, "v", false, "OptVerbose mode (equivalent to --log-level debug)")
	cmd.PersistentFlags().String(config.OptLoggingLevel, "info", "Log level (debug, info, warn, error)")
	cmd.PersistentFlags().Bool(config.OptForceHTTP2, false, "OptForce HTTP/2")
	cmd.PersistentFlags().Int(config.OptMaxConnPerHost, 40, "Maximum number of (global) concurrent connections per host")
	cmd.PersistentFlags().StringP(config.OptOutputConsumer, "o", "file", "Output Consumer (file, tar, null)")
	cmd.PersistentFlags().String(config.OptPIDFile, defaultPidFilePath(), "PID file path")

	if err := config.AddFlagAlias(cmd, config.OptConcurrency, config.OptMaxChunks); err != nil {
		return err
	}

	if err := hideAndDeprecateFlags(cmd); err != nil {
		return err
	}

	return nil
}

func hideAndDeprecateFlags(cmd *cobra.Command) error {
	// Hide flags from help, these are intended to be used for testing/internal benchmarking/debugging only
	if err := config.HideFlags(cmd, config.OptForceHTTP2, config.OptMaxConnPerHost, config.OptOutputConsumer, config.OptPIDFile); err != nil {
		return err
	}

	// DeprecatedFlag flags
	err := config.DeprecateFlags(cmd,
		config.DeprecatedFlag{Flag: config.OptMaxChunks, Msg: "use --concurrency instead"},
	)
	if err != nil {
		return err
	}
	return nil

}

func rootCmdPreRun(cmd *cobra.Command, args []string) {
	if viper.GetBool(config.OptExtract) {
		viper.Set(config.OptOutputConsumer, config.ConsumerTarExtractor)
	}
}

func runRootCMD(cmd *cobra.Command, args []string) error {
	// After we run through the PreRun functions we want to silence usage from being printed
	// on all errors
	cmd.SilenceUsage = true

	urlString := args[0]
	dest := args[1]

	log.Info().Str("url", urlString).
		Str("dest", dest).
		Str("minimum_chunk_size", viper.GetString(config.OptMinimumChunkSize)).
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
	minChunkSize, err := humanize.ParseBytes(viper.GetString(config.OptMinimumChunkSize))
	if err != nil {
		return fmt.Errorf("error parsing minimum chunk size: %w", err)
	}

	resolveOverrides, err := config.ResolveOverridesToMap(viper.GetStringSlice(config.OptResolve))
	if err != nil {
		return fmt.Errorf("error parsing resolve overrides: %w", err)
	}
	clientOpts := client.Options{
		ForceHTTP2:       viper.GetBool(config.OptForceHTTP2),
		MaxRetries:       viper.GetInt(config.OptRetries),
		ConnectTimeout:   viper.GetDuration(config.OptConnTimeout),
		MaxConnPerHost:   viper.GetInt(config.OptMaxConnPerHost),
		ResolveOverrides: resolveOverrides,
	}

	downloadOpts := download.Options{
		MaxConcurrency: viper.GetInt(config.OptConcurrency),
		MinChunkSize:   int64(minChunkSize),
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
		downloadOpts.CacheableURIPrefixAliases = config.GetURIAliases(downloadOpts.CacheableURIPrefixes)
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

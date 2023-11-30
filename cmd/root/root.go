package root

import (
	"context"
	"fmt"
	"os"

	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	pget "github.com/replicate/pget/pkg"
	"github.com/replicate/pget/pkg/cli"
	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/consumer"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/optname"
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

func GetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pget [flags] <url> <dest>",
		Short: "pget",
		Long:  rootLongDesc,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return config.PersistentStartupProcessFlags()
		},
		RunE:    runRootCMD,
		Args:    cobra.ExactArgs(2),
		Example: `  pget https://example.com/file.tar.gz file.tar.gz`,
	}
	cmd.Flags().BoolP(optname.Extract, "x", false, "Extract archive after download")
	cmd.SetUsageTemplate(cli.UsageTemplate)
	err := config.AddRootPersistentFlags(cmd)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return cmd
}

func runRootCMD(cmd *cobra.Command, args []string) error {
	// After we run through the PreRun functions we want to silence usage from being printed
	// on all errors
	cmd.SilenceUsage = true

	urlString := args[0]
	dest := args[1]

	log.Info().Str("url", urlString).
		Str("dest", dest).
		Str("minimum_chunk_size", viper.GetString(optname.MinimumChunkSize)).
		Msg("Initiating")

	if err := cli.EnsureDestinationNotExist(dest); err != nil {
		return err
	}

	if err := rootExecute(cmd.Context(), urlString, dest); err != nil {
		return err
	}

	return nil
}

// rootExecute is the main function of the program and encapsulates the general logic
// returns any/all errors to the caller.
func rootExecute(ctx context.Context, urlString, dest string) error {
	minChunkSize, err := humanize.ParseBytes(viper.GetString(optname.MinimumChunkSize))
	if err != nil {
		return err
	}

	clientOpts := client.Options{
		ForceHTTP2:     viper.GetBool(optname.ForceHTTP2),
		MaxRetries:     viper.GetInt(optname.Retries),
		ConnectTimeout: viper.GetDuration(optname.ConnTimeout),
	}
	downloadOpts := download.Options{
		MaxChunks:    viper.GetInt(optname.Concurrency),
		MinChunkSize: int64(minChunkSize),
		Client:       clientOpts,
	}
	getter := pget.Getter{
		Downloader: download.GetBufferMode(downloadOpts),
	}

	if viper.GetBool(optname.Extract) {
		getter.Consumer = &consumer.TarExtractor{}
	}

	_, _, err = getter.DownloadFile(ctx, urlString, dest)
	return err
}

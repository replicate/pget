package cmd

import (
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/optname"

	"github.com/replicate/pget/pkg/config"
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

var RootCMD = &cobra.Command{
	Use:   "pget [flags] <url> <dest>",
	Short: "pget",
	Long:  rootLongDesc,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.PersistentStartupProcessFlags()
	},
	Run: func(cmd *cobra.Command, args []string) {
		if err := rootExecFunc(cmd, args); err != nil {
			log.Error().Err(err).Msg("Error")
			os.Exit(1)
		}
	},
	Args: cobra.ExactArgs(2),
}

func init() {
	config.AddFlags(RootCMD)
}

// rootExecFunc is the main function of the program and encapsulates the general logic
// returns any/all errors to the caller.
func rootExecFunc(cmd *cobra.Command, args []string) error {
	url := args[0]
	dest := args[1]
	_, fileExists := os.Stat(dest)

	log.Info().Str("url", url).
		Str("dest", dest).
		Str("minimum_chunk_size", viper.GetString(optname.MinimumChunkSize)).
		Msg("Initiating")
	// ensure dest does not exist
	if !viper.GetBool(optname.Force) && !os.IsNotExist(fileExists) {
		return fmt.Errorf("destination %s already exists", dest)

	}

	// allows us to see how many pget procs are running at a time
	tmpFile := fmt.Sprintf("/tmp/.pget-%d", os.Getpid())
	_ = os.WriteFile(tmpFile, []byte(""), 0644)
	defer os.Remove(tmpFile)

	mode := download.GetMode(config.Mode)
	return mode.DownloadFile(url, dest)
}

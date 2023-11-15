package cmd

import (
	"github.com/spf13/cobra"

	"github.com/replicate/pget/pkg/config"
)

const longDesc = `
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
	Long:  longDesc,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.PersistentStartupProcessFlags()
	},
	Args: cobra.ExactArgs(2),
}
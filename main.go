package main

import (
	"fmt"

	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/replicate/pget/config"
	"github.com/replicate/pget/download"
	"github.com/replicate/pget/extract"
	"github.com/replicate/pget/optname"
	"github.com/replicate/pget/version"
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

func main() {
	cmd := &cobra.Command{
		Use:   "pget [flags] <url> <dest>",
		Short: "pget",
		Long:  longDesc,
		RunE:  mainFunc,
		Args:  cobra.ExactArgs(2),
	}
	config.AddFlags(cmd)
	cmd.AddCommand(version.CMDVersion)
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func mainFunc(cmd *cobra.Command, args []string) error {
	verboseMode := viper.GetBool(optname.Verbose)

	url := args[0]
	dest := args[1]
	_, fileExists := os.Stat(dest)

	if verboseMode {
		absPath, _ := filepath.Abs(dest)
		fmt.Println("URL:", url)
		fmt.Println("Destination:", absPath)
		fmt.Println("Minimum Chunk Size:", viper.GetString(optname.MinimumChunkSize))
		fmt.Println()
	}
	// ensure dest does not exist
	if !viper.GetBool(optname.Force) && !os.IsNotExist(fileExists) {
		return fmt.Errorf("destination %s already exists", dest)

	}

	// allows us to see how many pget procs are running at a time
	tmpFile := fmt.Sprintf("/tmp/.pget-%d", os.Getpid())
	os.WriteFile(tmpFile, []byte(""), 0644)
	defer os.Remove(tmpFile)

	buffer, fileSize, err := download.FileToBuffer(url)
	if err != nil {
		return fmt.Errorf("error downloading file: %v", err)
	}

	// extract the tar file if the -x flag was provided
	if viper.GetBool(optname.Extract) {
		err = extract.ExtractTarFile(buffer, dest, fileSize)
		if err != nil {
			return fmt.Errorf("error extracting file: %v", err)
		}
	} else {
		// if -x flag is not set, save the buffer to a file
		err = os.WriteFile(dest, buffer.Bytes(), 0644)
		if err != nil {
			return fmt.Errorf("error writing file: %v", err)
		}
	}
	return nil
}

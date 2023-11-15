package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/replicate/pget/cmd"
	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/extract"
	"github.com/replicate/pget/pkg/optname"
)

func main() {
	config.AddFlags(cmd.RootCMD)
	cmd.RootCMD.RunE = mainFunc
	cmd.RootCMD.AddCommand(cmd.VersionCMD)
	if err := cmd.RootCMD.Execute(); err != nil {
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
	_ = os.WriteFile(tmpFile, []byte(""), 0644)
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

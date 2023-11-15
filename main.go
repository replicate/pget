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
	"github.com/replicate/pget/pkg/optname"
)

func main() {
	config.AddFlags(cmd.RootCMD)
	cmd.RootCMD.Run = func(cmd *cobra.Command, args []string) {
		if err := execFunc(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	if err := cmd.RootCMD.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// execFunc is the main function of the program and encapsulates the general logic
// returns any/all errors to the caller.
func execFunc(cmd *cobra.Command, args []string) error {
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

	mode := download.GetMode(config.Mode)
	return mode.DownloadFile(url, dest)
}

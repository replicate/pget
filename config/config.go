package config

import (
	"github.com/replicate/pget/optname"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"runtime"
	"strings"
)

var (
	Concurrency      int
	Extract          bool
	Force            bool
	MinimumChunkSize string
	Retries          int
	Verbose          bool
)

func AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().IntVarP(&Concurrency, optname.Concurrency, "c", runtime.GOMAXPROCS(0)*4, "Maximum number of concurrent downloads")
	cmd.PersistentFlags().BoolVarP(&Extract, optname.Extract, "x", false, "Extract archive after download")
	cmd.PersistentFlags().StringVarP(&MinimumChunkSize, optname.MinimumChunkSize, "m", "16M", "Minimum chunk size (in bytes) to use when downloading a file (e.g. 10M)")
	cmd.PersistentFlags().BoolVarP(&Force, optname.Force, "f", false, "Force download, overwriting existing file")
	cmd.PersistentFlags().IntVarP(&Retries, optname.Retries, "r", 5, "Number of retries when attempting to retrieve a file")
	cmd.PersistentFlags().BoolVarP(&Verbose, optname.Verbose, "v", false, "Verbose mode")

	viper.SetEnvPrefix("PGET")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		panic(err)
	}
}

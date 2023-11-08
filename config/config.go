package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"runtime"
	"strings"
)

const MinChunkSizeOptName = "minimum-chunk-size"

var (
	Concurrency      int
	Retries          int
	Verbose          bool
	Force            bool
	MinimumChunkSize string

	envSupersedesFlags = []string{MinChunkSizeOptName}
)

func AddFlags(cmd *cobra.Command) {

	viper.SetEnvPrefix("R8GET")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	cmd.PersistentFlags().IntVarP(&Concurrency, "concurrency", "c", runtime.GOMAXPROCS(0)*4, "Maximum number of concurrent downloads")
	cmd.PersistentFlags().IntVarP(&Retries, "retries", "r", 5, "Number of retries when attempting to retrieve a file")
	cmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Verbose mode")
	cmd.PersistentFlags().BoolVarP(&Force, "force", "f", false, "Force download, overwriting existing file")
	cmd.PersistentFlags().StringVarP(&MinimumChunkSize, MinChunkSizeOptName, "m", "16M", "Minimum Chunk Size (e.g. 10M)")

	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		panic(err)
	}
	viper.AutomaticEnv()
}

package config

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"runtime"
	"strings"
)

var (
	Concurrency     int
	Retries         int
	Verbose         bool
	Force           bool
	TargetChunkSize HumanizedBytesValue
	RemoteName      bool
)

func AddFlags(cmd *cobra.Command) {
	// Set the default chunksize to 16M
	if err := TargetChunkSize.Set("16M"); err != nil {
		panic(err)
	}

	viper.SetEnvPrefix("pget")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	cmd.PersistentFlags().IntVarP(&Concurrency, "concurrency", "c", runtime.GOMAXPROCS(0)*4, "Maximum number of concurrent downloads")
	cmd.PersistentFlags().IntVarP(&Retries, "retries", "r", 5, "Number of retries when attempting to retrieve a file")
	cmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Verbose mode")
	cmd.PersistentFlags().BoolVarP(&Force, "force", "f", false, "Force download, overwriting existing file")
	cmd.PersistentFlags().VarP(&TargetChunkSize, "target-chunk-size", "m", "Target Chunk Size (e.g. 10M)")
	cmd.PersistentFlags().BoolVarP(&RemoteName, "remote-name", "O", false, "Use remote name for output file, use instead of the positional argument <dest>")

	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		panic(err)
	}
}

type HumanizedBytesValue int64

func (hb *HumanizedBytesValue) Set(value string) error {
	v, err := humanize.ParseBytes(value)
	*hb = HumanizedBytesValue(v)
	return err
}

func (hb *HumanizedBytesValue) String() string {
	return fmt.Sprintf("%d", *hb)
}

func (hb *HumanizedBytesValue) Type() string {
	return "humanizedBytes"
}

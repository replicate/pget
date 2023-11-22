package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/replicate/pget/pkg/version"
)

var VersionCMD = &cobra.Command{
	Use:   "version",
	Short: "print version and build information",
	Long:  "Print the version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("pget Version %s - Build Time %s\n", version.GetVersion(), version.BuildTime)
	},
}

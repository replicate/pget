package version

import (
	"fmt"
	"github.com/spf13/cobra"
)

var CMDVersion *cobra.Command = &cobra.Command{
	Use:   "version",
	Short: "Print the version information",
	Long:  "Print the version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("pget Version %s - Build Time %s\n", GetVersion(), BuildTime)
	},
}

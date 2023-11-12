package version

import (
	"fmt"

	"github.com/spf13/cobra"
)

const versionDev = "dev"

var (
	// Buuld-time injected variables
	Version    string
	CommitHash string
	BuildTime  string
)

// SubCommand to print version
var CmdVersion *cobra.Command = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of pget",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("pget version %s - built at %s\n", GetVersion(), BuildTime)
	},
}

func GetVersion() string {
	if Version == versionDev {
		return Version
	}
	if Version == CommitHash {
		return fmt.Sprintf("Commit %s", Version)
	}
	return fmt.Sprintf("%s (%s)", Version, CommitHash)
}

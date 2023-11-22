package cmd

import (
	"github.com/spf13/cobra"

	"github.com/replicate/pget/cmd/multifile"
	"github.com/replicate/pget/cmd/root"
)

func GetRootCommand() *cobra.Command {
	rootCMD := root.GetCommand()
	rootCMD.AddCommand(multifile.GetCommand())
	rootCMD.AddCommand(VersionCMD)
	return rootCMD
}

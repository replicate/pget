package cmd

import (
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/optname"

	"github.com/replicate/pget/pkg/config"
)

const rootLongDesc = `
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

const usageTemplate = `
Usage:{{if .Runnable}}
{{if .HasAvailableFlags}}{{appendIfNotPresent .UseLine "[flags]"}}{{else}}{{.UseLine}}{{end}}{{end}}{{if .HasAvailableSubCommands}}
{{.CommandPath}} [command]{{end}}{{if gt .Aliases 0}}

Aliases:
{{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if .IsAvailableCommand}}
{{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
{{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`

var RootCMD = &cobra.Command{
	Use:   "pget [flags] <url> <dest>",
	Short: "pget",
	Long:  rootLongDesc,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.PersistentStartupProcessFlags()
	},
	RunE:    runRootCMD,
	Args:    cobra.ExactArgs(2),
	Example: `  pget https://example.com/file.tar.gz file.tar.gz`,
}

func init() {
	config.AddFlags(RootCMD)

	RootCMD.SetUsageTemplate(usageTemplate)
	RootCMD.AddCommand(MultiFileCMD)
	RootCMD.AddCommand(VersionCMD)
}

func runRootCMD(cmd *cobra.Command, args []string) error {
	// After we run through the PreRun functions we want to silence usage from being printed
	// on all errors
	cmd.SilenceUsage = true

	urlString := args[0]
	dest := args[1]

	log.Info().Str("urlString", urlString).
		Str("dest", dest).
		Str("minimum_chunk_size", viper.GetString(optname.MinimumChunkSize)).
		Msg("Initiating")

	// ensure dest does not exist
	if err := fileExistsErr(dest); err != nil {
		return err
	}

	if err := rootExecute(urlString, dest); err != nil {
		return err
	}

	return nil
}

// rootExecute is the main function of the program and encapsulates the general logic
// returns any/all errors to the caller.
func rootExecute(urlString, dest string) error {
	// allows us to see how many pget procs are running at a time
	tmpFile := fmt.Sprintf("/tmp/.pget-%d", os.Getpid())
	_ = os.WriteFile(tmpFile, []byte(""), 0644)
	defer os.Remove(tmpFile)

	mode := download.GetMode(config.Mode)
	_, _, err := mode.DownloadFile(urlString, dest)
	return err
}

func fileExistsErr(dest string) error {
	_, err := os.Stat(dest)
	if !viper.GetBool(optname.Force) && !os.IsNotExist(err) {
		return fmt.Errorf("destination %s already exists", dest)
	}
	return nil
}

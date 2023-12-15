package main

import (
	"os"

	"github.com/replicate/pget/cmd"
	"github.com/replicate/pget/pkg/logging"
)

func main() {
	logging.SetupLogger()
	rootCMD := cmd.GetRootCommand()

	if err := rootCMD.Execute(); err != nil {
		os.Exit(1)
	}
}

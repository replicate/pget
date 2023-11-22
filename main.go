package main

import (
	"os"

	"github.com/replicate/pget/cmd"
)

func main() {
	rootCMD := cmd.GetRootCommand()
	if err := rootCMD.Execute(); err != nil {
		os.Exit(1)
	}
}

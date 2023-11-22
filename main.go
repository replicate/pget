package main

import (
	"os"

	"github.com/replicate/pget/cmd"
)

func main() {
	if err := cmd.RootCMD.Execute(); err != nil {
		os.Exit(1)
	}
}

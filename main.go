package main

import (
	"fmt"
	"os"

	"github.com/replicate/pget/cmd"
	"github.com/replicate/pget/pkg/config"
)

func main() {
	config.AddFlags(cmd.RootCMD)
	if err := cmd.RootCMD.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

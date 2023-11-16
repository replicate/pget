package main

import (
	"fmt"
	"os"

	"github.com/replicate/pget/cmd"
)

func main() {
	if err := cmd.RootCMD.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

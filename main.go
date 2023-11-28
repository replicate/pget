package main

import (
	"fmt"
	"os"

	"github.com/replicate/pget/cmd"
	"github.com/replicate/pget/pkg/logging"
)

func main() {
	logging.SetupLogger()
	rootCMD := cmd.GetRootCommand()

	// allows us to see how many pget procs are running at a time
	tmpFile := fmt.Sprintf("/tmp/.pget-%d", os.Getpid())
	_ = os.WriteFile(tmpFile, []byte(""), 0644)
	defer os.Remove(tmpFile)

	if err := rootCMD.Execute(); err != nil {
		os.Exit(1)
	}
}

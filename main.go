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

// Flock on PidFile to ensure only one pget process is running at a time.
// if pid file exists but does not have a Flock on it, check to see if the process is still running
// and is in-fact a pget process. If it is, then still acquire the Flock and block until the process
// ends.
func accquireFlock() {

}

package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"net"
	"runtime"
	"strings"
	"time"
)

const MinChunkSizeOptName = "minimum-chunk-size"

var (
	Concurrency      int
	Extract          bool
	Force            bool
	MinimumChunkSize string
	Retries          int
	Verbose          bool
	ResolveHosts     []string
	ConnTimeout      time.Duration

	HostToIPResolutionMap = make(map[string]string)
)

func AddFlags(cmd *cobra.Command) {

	viper.SetEnvPrefix("R8GET")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	cmd.PersistentFlags().IntVarP(&Concurrency, "concurrency", "c", runtime.GOMAXPROCS(0)*4, "Maximum number of concurrent downloads")
	cmd.PersistentFlags().IntVarP(&Retries, "retries", "r", 5, "Number of retries when attempting to retrieve a file")
	cmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Verbose mode")
	cmd.PersistentFlags().BoolVarP(&Force, "force", "f", false, "Force download, overwriting existing file")
	cmd.PersistentFlags().StringVarP(&MinimumChunkSize, MinChunkSizeOptName, "m", "16M", "Minimum Chunk Size (e.g. 10M)")
	cmd.PersistentFlags().BoolVarP(&Extract, "extract", "x", false, "Extract tar file after download")
	cmd.PersistentFlags().StringSliceVar(&ResolveHosts, "resolve", []string{}, "A mapping of Hostname to IP Address, format 'HostName:Port:IP'")
	cmd.PersistentFlags().DurationVar(&ConnTimeout, "connect-timeout", 5*time.Second, "Connection Timeout for each request, format is <number><unit> e.g. 10s")

	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		panic(err)
	}
	viper.AutomaticEnv()
}

func StartupProcessFlags() error {

	if err := convertResolveHostsToMap(); err != nil {
		return err
	}
	return nil

}

func convertResolveHostsToMap() error {
	for _, resolveHost := range ResolveHosts {
		var IPAddr *net.IPAddr
		split := strings.SplitN(resolveHost, ":", 3)
		if len(split) != 3 {
			return fmt.Errorf("invalid resolve host format, expected <hostname>:port:<ip>, got: %s", resolveHost)
		}
		host, port, addr := strings.ToLower(split[0]), split[1], split[2]
		if _, ok := HostToIPResolutionMap[host]; ok {
			return fmt.Errorf("duplicate hostname specified: %s", split[0])
		}
		IP := net.ParseIP(addr)
		if IPAddr == nil {
			return fmt.Errorf("invalid IP address: %s", split[1])
		}
		if IP.To16() != nil && IP.To4() == nil {
			return fmt.Errorf("invalid IP address: %s", split[1])
		}
		HostToIPResolutionMap[fmt.Sprintf("%s:%s", host, port)] = fmt.Sprintf("%s:%s", IPAddr.String(), port)
	}
	return nil
}

func LookupHost(addr string) string {
	addr = strings.ToLower(addr)
	if addrStr, ok := HostToIPResolutionMap[addr]; ok {
		return addrStr
	}
	return ""
}

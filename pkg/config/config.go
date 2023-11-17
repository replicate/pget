package config

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/logging"
	"github.com/replicate/pget/pkg/optname"
)

var (
	Concurrency      int
	ConnTimeout      time.Duration
	Extract          bool
	Force            bool
	LoggingLevel     string
	MinimumChunkSize string
	Mode             string
	ResolveHosts     []string
	Retries          int
	Verbose          bool
)

// HostToIPResolutionMap is a map of hostnames to IP addresses
var HostToIPResolutionMap = make(map[string]string)

func AddFlags(cmd *cobra.Command) {
	// Non-Persistent Flags (only applies to rootCMD)
	cmd.Flags().IntVarP(&Concurrency, optname.Concurrency, "c", runtime.GOMAXPROCS(0)*4, "Maximum number of concurrent downloads")
	cmd.Flags().BoolVarP(&Extract, optname.Extract, "x", false, "Extract archive after download")
	// Persistent Flags (applies to all commands/subcommands)
	cmd.PersistentFlags().DurationVar(&ConnTimeout, optname.ConnTimeout, 5*time.Second, "Timeout for establishing a connection, format is <number><unit>, e.g. 10s")
	cmd.PersistentFlags().StringVarP(&MinimumChunkSize, optname.MinimumChunkSize, "m", "16M", "Minimum chunk size (in bytes) to use when downloading a file (e.g. 10M)")
	cmd.PersistentFlags().BoolVarP(&Force, optname.Force, "f", false, "Force download, overwriting existing file")
	cmd.PersistentFlags().StringSliceVar(&ResolveHosts, optname.Resolve, []string{}, "Resolve hostnames to specific IPs")
	cmd.PersistentFlags().IntVarP(&Retries, optname.Retries, "r", 5, "Number of retries when attempting to retrieve a file")
	cmd.PersistentFlags().BoolVarP(&Verbose, optname.Verbose, "v", false, "Verbose mode (equivalent to --log-level debug")
	cmd.PersistentFlags().StringVar(&LoggingLevel, optname.LoggingLevel, "info", "Log level (debug, info, warn, error)")

	viper.SetEnvPrefix("PGET")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		panic(err)
	}
	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		panic(err)
	}
}

func PersistentStartupProcessFlags() error {
	Mode = "buffer"
	if viper.GetBool(optname.Verbose) {
		viper.Set(optname.LoggingLevel, "debug")
	}
	// Set log-level
	switch strings.ToLower(viper.GetString(optname.LoggingLevel)) {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	if viper.GetBool(optname.Extract) {
		Mode = "tar-extract"
	}
	if err := convertResolveHostsToMap(); err != nil {
		return err
	}
	return nil

}

func convertResolveHostsToMap() error {
	for _, resolveHost := range viper.GetStringSlice(optname.Resolve) {
		split := strings.SplitN(resolveHost, ":", 3)
		if len(split) != 3 {
			return fmt.Errorf("invalid resolve host format, expected <hostname>:port:<ip>, got: %s", resolveHost)
		}
		host, port, addr := split[0], split[1], split[2]
		if net.ParseIP(host) != nil {
			return fmt.Errorf("invalid hostname specified, looks like an IP address: %s", host)
		}
		hostPort := net.JoinHostPort(host, port)
		if _, ok := HostToIPResolutionMap[hostPort]; ok {
			return fmt.Errorf("duplicate host:port specified: %s", host)
		}
		if net.ParseIP(addr) == nil {
			return fmt.Errorf("invalid IP address: %s", addr)
		}
		HostToIPResolutionMap[hostPort] = net.JoinHostPort(addr, port)
	}
	if logging.Logger.GetLevel() == zerolog.DebugLevel {
		for key, elem := range HostToIPResolutionMap {
			logging.Logger.Debug().Str("host_port", key).Str("resolve_target", elem).Msg("Config")
		}
	}
	return nil
}

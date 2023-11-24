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
	ConnTimeout      time.Duration
	Force            bool
	ForceHTTP2       bool
	IgnoreRetryAfter bool
	LoggingLevel     string
	MaxChunkNumber   int
	MinimumChunkSize string
	ResolveHosts     []string
	Retries          int
	Verbose          bool
)

var logger = logging.GetLogger()

// HostToIPResolutionMap is a map of hostnames to IP addresses
var HostToIPResolutionMap = make(map[string]string)

func AddRootPersistentFlags(cmd *cobra.Command) error {
	// Persistent Flags (applies to all commands/subcommands)
	cmd.PersistentFlags().IntVar(&MaxChunkNumber, optname.Concurrency, runtime.GOMAXPROCS(0)*4, "Maximum number of concurrent downloads/maximum number of chunks for a given file (alias for --max-chunks)")
	cmd.PersistentFlags().IntVarP(&MaxChunkNumber, optname.MaxChunks, "c", runtime.GOMAXPROCS(0)*4, "Maximum number of chunks for a given file")
	cmd.PersistentFlags().DurationVar(&ConnTimeout, optname.ConnTimeout, 5*time.Second, "Timeout for establishing a connection, format is <number><unit>, e.g. 10s")
	cmd.PersistentFlags().StringVarP(&MinimumChunkSize, optname.MinimumChunkSize, "m", "16M", "Minimum chunk size (in bytes) to use when downloading a file (e.g. 10M)")
	cmd.PersistentFlags().BoolVarP(&Force, optname.Force, "f", false, "Force download, overwriting existing file")
	cmd.PersistentFlags().StringSliceVar(&ResolveHosts, optname.Resolve, []string{}, "Resolve hostnames to specific IPs")
	cmd.PersistentFlags().IntVarP(&Retries, optname.Retries, "r", 5, "Number of retries when attempting to retrieve a file")
	cmd.PersistentFlags().BoolVarP(&Verbose, optname.Verbose, "v", false, "Verbose mode (equivalent to --log-level debug)")
	cmd.PersistentFlags().StringVar(&LoggingLevel, optname.LoggingLevel, "info", "Log level (debug, info, warn, error)")
	cmd.PersistentFlags().BoolVar(&IgnoreRetryAfter, optname.IgnoreRetryAfter, false, "Ignore Retry-After header")
	cmd.PersistentFlags().BoolVar(&ForceHTTP2, optname.ForceHTTP2, false, "Force HTTP/2")

	viper.SetEnvPrefix("PGET")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		panic(err)
	}
	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		panic(err)
	}
	viper.RegisterAlias(optname.Concurrency, optname.MaxChunks)

	// Hide flags from help, these are intended to be used for testing/internal benchmarking/debugging only
	if err := cmd.PersistentFlags().MarkHidden(optname.ForceHTTP2); err != nil {
		return fmt.Errorf("failed to hide flag %s: %w", optname.ForceHTTP2, err)
	}

	return nil
}

func PersistentStartupProcessFlags() error {
	if viper.GetBool(optname.Verbose) {
		viper.Set(optname.LoggingLevel, "debug")
	}
	setLogLevel(viper.GetString(optname.LoggingLevel))
	if err := convertResolveHostsToMap(); err != nil {
		return err
	}
	return nil

}

func setLogLevel(logLevel string) {
	// Set log-level
	switch logLevel {
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
	if logger.GetLevel() == zerolog.DebugLevel {
		for key, elem := range HostToIPResolutionMap {
			logger.Debug().Str("host_port", key).Str("resolve_target", elem).Msg("Config")
		}
	}
	return nil
}

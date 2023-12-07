package config

import (
	"fmt"
	"net"
	"strings"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/consumer"
	"github.com/replicate/pget/pkg/logging"
)

const viperEnvPrefix = "PGET"

const (
	ConsumerFile         = "file"
	ConsumerTarExtractor = "tar-extractor"
	ConsumerNull         = "null"
)

type DeprecatedFlag struct {
	Flag string
	Msg  string
}

// HostToIPResolutionMap is a map of hostnames to IP addresses
// TODO: Eliminate this global variable
var HostToIPResolutionMap = make(map[string]string)

func PersistentStartupProcessFlags() error {
	if viper.GetBool(OptVerbose) {
		viper.Set(OptLoggingLevel, "debug")
	}
	setLogLevel(viper.GetString(OptLoggingLevel))
	if err := convertResolveHostsToMap(); err != nil {
		return err
	}
	return nil

}

func HideFlags(cmd *cobra.Command, flags ...string) error {
	for _, flag := range flags {
		f := cmd.Flag(flag)
		if f == nil {
			return fmt.Errorf("flag %s does not exist", flag)
		}
		// Try hiding a non-persistent flag, if it doesn't exist, try hiding a persistent flag of the same name
		// this is similar to how cobra implements the .Flag() lookup
		err := cmd.Flags().MarkHidden(flag)
		if err != nil {
			// We shouldn't be able to get an error here because we check f := cmd.Flag(flag) which does the
			// check across both persistent and non-persistent flags
			_ = cmd.PersistentFlags().MarkHidden(flag)
		}
	}
	return nil
}

func DeprecateFlags(cmd *cobra.Command, deprecations ...DeprecatedFlag) error {
	for _, config := range deprecations {
		f := cmd.Flag(config.Flag)
		if f == nil {
			return fmt.Errorf("flag %s does not exist", config.Flag)
		}
		err := cmd.Flags().MarkDeprecated(config.Flag, config.Msg)
		if err != nil {
			return fmt.Errorf("failed to mark flag as deprecated: %w", err)
		}
	}
	return nil
}

func AddFlagAlias(cmd *cobra.Command, alias, flag string) error {
	f := cmd.Flag(flag)
	if f == nil {
		return fmt.Errorf("flag %s does not exist", flag)
	}

	viper.RegisterAlias(alias, flag)
	return nil
}

func ViperInit() {
	viper.SetEnvPrefix(viperEnvPrefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
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
	logger := logging.GetLogger()
	for _, resolveHost := range viper.GetStringSlice(OptResolve) {
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
		logger := logging.GetLogger()

		for key, elem := range HostToIPResolutionMap {
			logger.Debug().Str("host_port", key).Str("resolve_target", elem).Msg("Config")
		}
	}
	return nil
}

// GetConsumer returns the consumer specified by the user on the command line
// or an error if the consumer is invalid. Note that this function explicitly
// calls viper.GetString(OptExtract) internally.
func GetConsumer() (consumer.Consumer, error) {
	consumerName := viper.GetString(OptOutputConsumer)
	switch consumerName {
	case ConsumerFile:
		return &consumer.FileWriter{}, nil
	case ConsumerTarExtractor:
		return &consumer.TarExtractor{}, nil
	case ConsumerNull:
		return &consumer.NullWriter{}, nil
	default:
		return nil, fmt.Errorf("invalid consumer specified: %s", consumerName)
	}
}

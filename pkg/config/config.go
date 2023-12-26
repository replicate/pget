package config

import (
	"fmt"
	"net"
	"net/url"
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

type ConsistentHashingStrategy struct{}

var ConsistentHashingStrategyKey ConsistentHashingStrategy

type DeprecatedFlag struct {
	Flag string
	Msg  string
}

func PersistentStartupProcessFlags() error {
	if viper.GetBool(OptVerbose) {
		viper.Set(OptLoggingLevel, "debug")
	}
	setLogLevel(viper.GetString(OptLoggingLevel))
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
			err := cmd.PersistentFlags().MarkDeprecated(config.Flag, config.Msg)
			if err != nil {
				return fmt.Errorf("failed to mark flag as deprecated: %w", err)
			}
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

func ResolveOverridesToMap(resolveOverrides []string) (map[string]string, error) {
	logger := logging.GetLogger()
	resolveOverrideMap := make(map[string]string)

	if len(resolveOverrides) == 0 {
		return nil, nil
	}

	for _, resolveHost := range resolveOverrides {
		split := strings.SplitN(resolveHost, ":", 3)
		if len(split) != 3 {
			return nil, fmt.Errorf("invalid resolve host format, expected <hostname>:port:<ip>, got: %s", resolveHost)
		}
		host, port, addr := split[0], split[1], split[2]
		if net.ParseIP(host) != nil {
			return nil, fmt.Errorf("invalid hostname specified, looks like an IP address: %s", host)
		}
		hostPort := net.JoinHostPort(host, port)
		if override, ok := resolveOverrideMap[hostPort]; ok {
			if override == net.JoinHostPort(addr, port) {
				// duplicate entry, ignore
				continue
			}
			return nil, fmt.Errorf("duplicate host:port specified: %s", host)
		}
		if net.ParseIP(addr) == nil {
			return nil, fmt.Errorf("invalid IP address: %s", addr)
		}
		resolveOverrideMap[hostPort] = net.JoinHostPort(addr, port)
	}
	if logger.GetLevel() == zerolog.DebugLevel {
		logger := logging.GetLogger()

		for key, elem := range resolveOverrideMap {
			logger.Debug().Str("host_port", key).Str("resolve_target", elem).Msg("Config")
		}
	}
	return resolveOverrideMap, nil
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

// GetCacheSRV returns the SRV name of the cache to use, if set.
func GetCacheSRV() string {
	if srv := viper.GetString(OptCacheNodesSRVName); srv != "" {
		return srv
	}
	hostIP := net.ParseIP(viper.GetString(OptHostIP))
	srvNamesByCIDR := viper.GetStringMapString(OptCacheNodesSRVNameByHostCIDR)
	if hostIP == nil {
		// nothing configured, return zero value with no error
		return ""
	}
	for cidr, cidrSRV := range srvNamesByCIDR {
		_, net, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if net.Contains(hostIP) {
			return cidrSRV
		}
	}
	return ""
}

// CacheableURIPrefixes returns a map of cache URI prefixes to send through consistent hash, if set.
func CacheableURIPrefixes() map[string][]*url.URL {
	logger := logging.GetLogger()
	result := make(map[string][]*url.URL)

	URIs := viper.GetStringMapString(OptCacheURIPrefixes)
	for key, uri := range URIs {
		parsed, err := url.Parse(uri)
		if err != nil {
			logger.Error().Err(err).Str("uri", uri).Msg("Cacheable URI Prefixes")
		}
		result[key] = append(result[key], parsed)
	}
	return result
}

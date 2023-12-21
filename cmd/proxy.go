package cmd

import (
	"fmt"
	"os"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/cli"
	"github.com/replicate/pget/pkg/client"
	"github.com/replicate/pget/pkg/config"
	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/proxy"
)

const longDesc = `
TODO
`

func GetProxyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "proxy [flags] <url> <dest>",
		Short:   "run as an http proxy server",
		Long:    longDesc,
		PreRunE: proxyPreRunE,
		RunE:    runProxyCMD,
		Args:    cobra.ExactArgs(0),
		Example: `  pget proxy`,
	}
	cmd.Flags().String(config.OptListenAddress, "127.0.0.1:9512", "address to listen on")
	err := viper.BindPFlags(cmd.PersistentFlags())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	cmd.SetUsageTemplate(cli.UsageTemplate)
	return cmd
}

func proxyPreRunE(cmd *cobra.Command, args []string) error {
	if viper.GetBool(config.OptExtract) {
		return fmt.Errorf("cannot use --extract with proxy mode")
	}
	if viper.GetString(config.OptOutputConsumer) == config.ConsumerTarExtractor {
		return fmt.Errorf("cannot use --output-consumer tar-extractor with proxy mode")
	}
	return nil
}

func runProxyCMD(cmd *cobra.Command, args []string) error {
	minChunkSize, err := humanize.ParseBytes(viper.GetString(config.OptMinimumChunkSize))
	if err != nil {
		return err
	}
	clientOpts := client.Options{
		MaxConnPerHost: viper.GetInt(config.OptMaxConnPerHost),
		ForceHTTP2:     viper.GetBool(config.OptForceHTTP2),
		MaxRetries:     viper.GetInt(config.OptRetries),
		ConnectTimeout: viper.GetDuration(config.OptConnTimeout),
	}
	downloadOpts := download.Options{
		MaxConcurrency: viper.GetInt(config.OptConcurrency),
		MinChunkSize:   int64(minChunkSize),
		Client:         clientOpts,
	}

	// TODO DRY this
	srvName := config.GetCacheSRV()

	if srvName == "" {
		return fmt.Errorf("Option %s MUST be specified in proxy mode", config.OptCacheNodesSRVName)
	}

	downloadOpts.SliceSize = 500 * humanize.MiByte
	// FIXME: make this a config option
	downloadOpts.DomainsToCache = []string{"weights.replicate.delivery"}
	// TODO: dynamically respond to SRV updates rather than just looking up
	// once at startup
	downloadOpts.CacheHosts, err = cli.LookupCacheHosts(srvName)
	if err != nil {
		return err
	}
	chMode, err := download.GetConsistentHashingMode(downloadOpts)
	if err != nil {
		return err
	}

	proxy, err := proxy.New(
		chMode,
		&proxy.Options{
			Address: viper.GetString(config.OptListenAddress),
		})
	if err != nil {
		return err
	}
	return proxy.Start()
}

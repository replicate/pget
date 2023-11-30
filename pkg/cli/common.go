package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/optname"
)

const UsageTemplate = `
Usage:{{if .Runnable}}
{{if .HasAvailableFlags}}{{appendIfNotPresent .UseLine "[flags]"}}{{else}}{{.UseLine}}{{end}}{{end}}{{if .HasAvailableSubCommands}}
{{.CommandPath}} [command]{{end}}{{if gt .Aliases 0}}

Aliases:
{{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if .IsAvailableCommand}}
{{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
{{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`

func EnsureDestinationNotExist(dest string) error {
	_, err := os.Stat(dest)
	if !viper.GetBool(optname.Force) && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("destination %s already exists", dest)
	}
	return nil
}

func LookupCacheHosts(srvName string) ([]string, error) {
	_, srvs, err := net.LookupSRV("http", "tcp", srvName)
	if err != nil {
		return nil, err
	}
	return orderCacheHosts(srvs)
}

var hostnameIndexRegexp = regexp.MustCompile(`^[a-z0-9-]*-([0-9]+)[.]`)

func orderCacheHosts(srvs []*net.SRV) ([]string, error) {
	// loop through to find highest index
	highestIndex := 0
	for _, srv := range srvs {
		cacheIndex, err := cacheIndexFor(srv.Target)
		if err != nil {
			return nil, err
		}
		if cacheIndex > highestIndex {
			highestIndex = cacheIndex
		}
	}
	output := make([]string, highestIndex+1)
	for _, srv := range srvs {
		cacheIndex, err := cacheIndexFor(srv.Target)
		if err != nil {
			return nil, err
		}
		hostname := strings.TrimSuffix(srv.Target, ".")
		if srv.Port != 80 {
			hostname = fmt.Sprintf("%s:%d", hostname, srv.Port)
		}
		output[cacheIndex] = hostname
	}
	return output, nil
}

func cacheIndexFor(hostname string) (int, error) {
	matches := hostnameIndexRegexp.FindStringSubmatch(hostname)
	if matches == nil {
		return -1, fmt.Errorf("couldn't parse hostname %s", hostname)
	}
	return strconv.Atoi(matches[1])
}

package version

import (
	"fmt"
	"strings"
)

var (
	// Version Build Time Injected information
	Version    string
	CommitHash string
	BuildTime  string
)

// GetVersion returns the version information in a human consumable way. This is intended to be used
// when the user requests the version information or in the case of the User-Agent.
func GetVersion() string {
	if strings.HasPrefix(Version, "development") {
		// This is a development build
		return Version

	}
	// This should be a tagged release
	return fmt.Sprintf("%s(%s)", Version, CommitHash)

}

package version

import "fmt"

const (
	snapshotString = "snapshot"
)

var (
	// Version Build Time Injected information
	Version    string
	CommitHash string
	BuildTime  string
	Prerelease string
	Snapshot   string
	OS         string
	Arch       string
	Branch     string
)

// GetVersion returns the version information in a human consumable way. This is intended to be used
// when the user requests the version information or in the case of the User-Agent.
func GetVersion() string {
	return makeVersionString(Version, CommitHash, BuildTime, Prerelease, Snapshot, OS, Arch, Branch)
}

func makeVersionString(version, commitHash, buildtime, prerelease, snapshot, os, arch, branch string) (versionString string) {
	versionString = fmt.Sprintf("%s(%s)", version, commitHash)
	if prerelease != "" {
		versionString = fmt.Sprintf("%s-%s", versionString, prerelease)
	} else if snapshot == "true" {
		versionString = fmt.Sprintf("%s-%s", versionString, snapshotString)
	}

	if branch != "" && branch != "main" && branch != "HEAD" {
		versionString = fmt.Sprintf("%s[%s]", versionString, branch)
	}

	if os != "" && arch != "" {
		versionString = fmt.Sprintf("%s/%s-%s", versionString, os, arch)
	} else if os != "" {
		versionString = fmt.Sprintf("%s/%s", versionString, os)
	}

	return versionString
}

package version

import (
	"testing"
)

func TestGetVersion(t *testing.T) {
	defer func() {
		Version = ""
		CommitHash = ""
		BuildTime = ""
	}()

	testCases := []struct {
		name     string
		version  string
		commit   string
		expected string
	}{
		{"development", "development", "", "development"},
		{"tagged release", "v1.0.0", "deadbeef", "v1.0.0()"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			Version = tc.version
			actual := GetVersion()
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

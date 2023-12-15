package version

import (
	"testing"
)

func Test_makeVersionString(t *testing.T) {
	type args struct {
		version    string
		commitHash string
		buildtime  string
		prerelease string
		snapshot   string
		os         string
		arch       string
		branch     string
	}
	tests := []struct {
		name     string
		args     args
		expected string
	}{
		{
			name: "Typical Development",
			args: args{
				version:    "1.0.0",
				commitHash: "abc123",
				os:         "darwin",
				arch:       "amd64",
				branch:     "Branch1",
			},
			expected: "1.0.0(abc123)[Branch1]/darwin-amd64",
		},
		{
			name: "With prerelease and snapshot",
			args: args{
				version:    "1.0.0",
				commitHash: "abc123",
				prerelease: "alpha",
				snapshot:   "20221130",
				os:         "darwin",
				arch:       "amd64",
				branch:     "Branch1",
			},
			expected: "1.0.0(abc123)-alpha[Branch1]/darwin-amd64",
		},
		{
			name: "No os or arch",
			args: args{
				version:    "1.0.0",
				commitHash: "abc123",
				branch:     "Branch1",
			},
			expected: "1.0.0(abc123)[Branch1]",
		},
		{
			name: "Branch Main",
			args: args{
				version:    "1.0.0",
				commitHash: "abc123",
				os:         "darwin",
				arch:       "amd64",
				branch:     "main",
			},
			expected: "1.0.0(abc123)/darwin-amd64",
		},
		{
			name: "Branch HEAD",
			args: args{
				version:    "1.0.0",
				commitHash: "abc123",
				os:         "darwin",
				arch:       "amd64",
				branch:     "HEAD",
			},
			expected: "1.0.0(abc123)/darwin-amd64",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := makeVersionString(tt.args.version, tt.args.commitHash, tt.args.buildtime, tt.args.prerelease, tt.args.snapshot, tt.args.os, tt.args.arch, tt.args.branch); got != tt.expected {
				t.Errorf("makeVersionString() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

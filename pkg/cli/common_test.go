package cli

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/replicate/pget/pkg/optname"
)

func TestEnsureDestinationNotExist(t *testing.T) {
	defer viper.Reset()
	f, err := os.CreateTemp("", "EnsureDestinationNotExist-test-file")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	testCases := []struct {
		name     string
		fileName string
		force    bool
		err      bool
	}{
		{"force true, file exists", f.Name(), true, false},
		{"force false, file exists", f.Name(), false, true},
		{"force true, file does not exist", f.Name(), true, false},
		{"force false, file does not exist", "unknownFile", false, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			viper.Set(optname.Force, tc.force)
			err := EnsureDestinationNotExist(tc.fileName)
			assert.Equal(t, tc.err, err != nil)
		})
	}
}

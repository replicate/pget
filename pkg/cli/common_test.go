package cli

import (
	"net"
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

type tc struct {
	srvs           []*net.SRV
	expectedOutput []string
}

var testCases = []tc{
	{ // basic functionality
		srvs:           []*net.SRV{{Target: "cache-0.cache-service.cache-namespace.svc.cluster.local.", Port: 80}},
		expectedOutput: []string{"cache-0.cache-service.cache-namespace.svc.cluster.local"},
	},
	{ // append port number if nonstandard
		srvs:           []*net.SRV{{Target: "cache-0.cache-service.cache-namespace.svc.cluster.local.", Port: 8080}},
		expectedOutput: []string{"cache-0.cache-service.cache-namespace.svc.cluster.local:8080"},
	},
	{ // multiple cache hosts
		srvs: []*net.SRV{
			{Target: "cache-0.cache-service.cache-namespace.svc.cluster.local.", Port: 80},
			{Target: "cache-1.cache-service.cache-namespace.svc.cluster.local.", Port: 80},
		},
		expectedOutput: []string{
			"cache-0.cache-service.cache-namespace.svc.cluster.local",
			"cache-1.cache-service.cache-namespace.svc.cluster.local",
		},
	},
	{ // canonical ordering
		srvs: []*net.SRV{
			{Target: "cache-1.cache-service.cache-namespace.svc.cluster.local.", Port: 80},
			{Target: "cache-0.cache-service.cache-namespace.svc.cluster.local.", Port: 80},
		},
		expectedOutput: []string{
			"cache-0.cache-service.cache-namespace.svc.cluster.local",
			"cache-1.cache-service.cache-namespace.svc.cluster.local",
		},
	},
	{ // ensure missing hosts are represented
		srvs: []*net.SRV{
			{Target: "cache-0.cache-service.cache-namespace.svc.cluster.local.", Port: 80},
			{Target: "cache-2.cache-service.cache-namespace.svc.cluster.local.", Port: 80},
		},
		expectedOutput: []string{
			"cache-0.cache-service.cache-namespace.svc.cluster.local",
			"",
			"cache-2.cache-service.cache-namespace.svc.cluster.local",
		},
	},
}

func TestOrderCacheHosts(t *testing.T) {
	for _, testCase := range testCases {
		cacheHosts, err := orderCacheHosts(testCase.srvs)
		assert.NoError(t, err)
		assert.Equal(t, testCase.expectedOutput, cacheHosts)
	}
}

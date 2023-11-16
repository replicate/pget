package client

import (
	"fmt"
	"sync"

	"github.com/spf13/viper"

	"github.com/replicate/pget/pkg/optname"
)

// perHostClientPool is a map of hostnames to a clientPool channel
var perHostClientPool = make(map[string]*perHostClientLimiter)
var createMutex = &sync.Mutex{}

// perHostClientLimiter is a semaphore that limits the number of concurrent connections per host
type perHostClientLimiter struct {
	pool chan *HTTPClient
}

func CreateHostConnectionPool(host string) {
	if viper.GetInt(optname.MaxConnPerHost) > 0 {
		createMutex.Lock()
		if _, ok := perHostClientPool[host]; !ok {
			perHostClientPool[host] = &perHostClientLimiter{pool: make(chan *HTTPClient, viper.GetInt(optname.MaxConnPerHost))}
			for c := 0; c < viper.GetInt(optname.MaxConnPerHost); c++ {
				perHostClientPool[host].pool <- newClient(host)
			}
		}
		createMutex.Unlock()
	}
}

func AcquireClient(host string) (*HTTPClient, error) {
	if viper.GetInt(optname.MaxConnPerHost) > 0 {
		if hostLimiter, ok := perHostClientPool[host]; ok {
			return <-hostLimiter.pool, nil
		}
		return nil, fmt.Errorf("connection pool found for host %s", host)
	}
	return newClient(host), nil
}

func releaseClient(client *HTTPClient) {
	if viper.GetInt(optname.MaxConnPerHost) > 0 {
		hostLimiter, ok := perHostClientPool[client.host]
		if !ok {
			// We should NEVER get here
			panic("connection pool not found for host " + client.host)
		}
		hostLimiter.pool <- client
	}
}

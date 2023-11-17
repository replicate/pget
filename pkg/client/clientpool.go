package client

import (
	"errors"
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
	maxConns := viper.GetInt(optname.MaxConnPerHost)
	if maxConns > 0 {
		createMutex.Lock()
		defer createMutex.Unlock()

		if _, ok := perHostClientPool[host]; !ok {
			perHostClientPool[host] = &perHostClientLimiter{pool: make(chan *HTTPClient, maxConns)}
			for c := 0; c < maxConns; c++ {
				perHostClientPool[host].pool <- newClient(host)
			}
		}
	}
}

func AcquireClient(host string) (*HTTPClient, error) {
	maxConnections := viper.GetInt(optname.MaxConnPerHost)
	// If maxConnections is not more than 0, we return a new client.
	if maxConnections <= 0 {
		return newClient(host), nil
	}

	// If host limiter is not found in the pool
	hostLimiter, ok := perHostClientPool[host]
	if !ok {
		return nil, fmt.Errorf("no connection pool found for host: %s", host)
	}

	// In case hostLimiter is found, we return a client from the pool.
	return <-hostLimiter.pool, nil
}

func releaseClient(client *HTTPClient) error {
	if client == nil || client.host == "" {
		return errors.New("invalid client")
	}

	if viper.GetInt(optname.MaxConnPerHost) > 0 {
		hostLimiter, ok := perHostClientPool[client.host]
		if !ok {
			return fmt.Errorf("connection pool not found for host %s", client.host)
		}

		hostLimiter.pool <- client
	}

	return nil
}

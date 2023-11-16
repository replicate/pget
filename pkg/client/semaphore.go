package client

import (
	"github.com/replicate/pget/pkg/optname"
	"github.com/spf13/viper"
)

// perHostClientLimiters is a map of hostnames to semaphores
var perHostClientLimiters = make(map[string]*perHostClientLimiter)

// perHostClientLimiter is a semaphore that limits the number of concurrent connections per host
type perHostClientLimiter struct {
	sem chan struct{}
}

func acquireHostSem(host string) {
	if viper.GetInt(optname.MaxConnPerHost) > 0 {
		if hostLimiter, ok := perHostClientLimiters[host]; ok {
			hostLimiter.sem <- struct{}{}
		} else {
			hostLimiter = &perHostClientLimiter{sem: make(chan struct{}, viper.GetInt(optname.MaxConnPerHost))}
			perHostClientLimiters[host] = hostLimiter
			hostLimiter.sem <- struct{}{}
		}
	}
}

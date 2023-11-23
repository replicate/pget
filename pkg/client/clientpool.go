package client

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/replicate/pget/pkg/version"
)

// perHostClientLimiter is a semaphore that limits the number of concurrent connections per host
type perHostClientLimiter struct {
	pool chan *HTTPClient
}

type ClientPool interface {
	// Do has the same contract as http.Client#Do.  If all clients are busy,
	// Do may block until a client becomes free.
	Do(req *http.Request) (*http.Response, error)
}

type clientPool struct {
	perHostClientPool map[string]*perHostClientLimiter
	clientPoolMutex   *sync.RWMutex
	maxConnsPerHost   int
}

var _ ClientPool = &clientPool{}

func NewClientPool(maxConnsPerHost int) ClientPool {
	perHostClientPool := make(map[string]*perHostClientLimiter)
	return &clientPool{
		perHostClientPool: perHostClientPool,
		clientPoolMutex:   &sync.RWMutex{},
		maxConnsPerHost:   maxConnsPerHost,
	}
}

func (p *clientPool) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", fmt.Sprintf("pget/%s", version.GetVersion()))

	if p.maxConnsPerHost == 0 {
		client := newClient()
		return client.Do(req)
	}
	schemeHost := getSchemeHostKey(req.URL)
	client, err := p.acquireClient(schemeHost)
	if err != nil {
		return nil, err
	}
	defer p.releaseClient(schemeHost, client)
	return client.Do(req)
}

func (p *clientPool) acquireClient(schemeHost string) (*HTTPClient, error) {
	p.clientPoolMutex.RLock()
	hostLimiter, ok := p.perHostClientPool[schemeHost]
	p.clientPoolMutex.RUnlock()
	if !ok {
		hostLimiter = &perHostClientLimiter{pool: make(chan *HTTPClient, p.maxConnsPerHost)}
		for c := 0; c < p.maxConnsPerHost; c++ {
			hostLimiter.pool <- newClient()
		}

		p.clientPoolMutex.Lock()
		// we need to check again to see if a concurrent goroutine has
		// won the race to create a client pool
		newHostLimiter, ok := p.perHostClientPool[schemeHost]
		if ok {
			// if we lost the race, use their hostLimiter and
			// discard ours
			hostLimiter = newHostLimiter
		} else {
			// otherwise, save ours to the client pool
			p.perHostClientPool[schemeHost] = hostLimiter
		}
		p.clientPoolMutex.Unlock()
	}

	return <-hostLimiter.pool, nil
}

func (p *clientPool) releaseClient(schemeHost string, client *HTTPClient) {
	p.clientPoolMutex.RLock()
	defer p.clientPoolMutex.RUnlock()
	p.perHostClientPool[schemeHost].pool <- client
}

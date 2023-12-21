package proxy

import (
	"net/http"
	"time"

	"github.com/replicate/pget/pkg/download"
)

type Proxy struct {
	httpServer *http.Server
	chMode     *download.ConsistentHashingMode
	opts       *Options
}

type Options struct {
	Address string
}

func New(chMode *download.ConsistentHashingMode, opts *Options) (*Proxy, error) {
	return &Proxy{
		chMode: chMode,
		opts:   opts,
	}, nil
}

func (p *Proxy) Start() error {
	var err error
	if err != nil {
		return err
	}
	p.httpServer = &http.Server{
		Addr:              p.opts.Address,
		Handler:           p.chMode,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
	}
	return p.httpServer.ListenAndServe()
}

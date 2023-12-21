package proxy

import (
	"net/http"
	"time"

	"github.com/replicate/pget/pkg/download"
	"github.com/replicate/pget/pkg/logging"
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
	logger := logging.GetLogger()
	var err error
	if err != nil {
		return err
	}
	logger.Debug().Str("address", p.opts.Address).Msg("Listening on")
	p.httpServer = &http.Server{
		Addr:              p.opts.Address,
		Handler:           p.chMode,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
	}
	return p.httpServer.ListenAndServe()
}

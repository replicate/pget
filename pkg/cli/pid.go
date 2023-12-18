//go:build !windows

package cli

import (
	"fmt"
	"os"
	"syscall"

	"github.com/replicate/pget/pkg/logging"
)

type PIDFile struct {
	file *os.File
	fd   int
}

func NewPIDFile(path string) (*PIDFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &PIDFile{file: file, fd: int(file.Fd())}, nil
}

func (p *PIDFile) Acquire() error {
	logger := logging.GetLogger()
	funcs := []func() error{
		func() error {
			logger.Debug().Str("blocking_lock_acquire", "false").Msg("Waiting on Lock")
			err := syscall.Flock(p.fd, syscall.LOCK_EX|syscall.LOCK_NB)
			if err != nil {
				logger.Warn().
					Err(err).
					Str("message", "Another pget process may be running, use 'pget multifile' to download multiple files in parallel").
					Msg("Waiting on Lock")
				logger.Debug().Str("blocking_lock_acquire", "true").Msg("Waiting on Lock")
				err = syscall.Flock(p.fd, syscall.LOCK_EX)
			}
			return err
		},
		p.writePID,
		p.file.Sync,
	}
	return p.executeFuncs(funcs)
}

func (p *PIDFile) Release() error {
	funcs := []func() error{
		func() error { return syscall.Flock(p.fd, syscall.LOCK_UN) },
		p.file.Close,
		func() error { return os.Remove(p.file.Name()) },
	}
	return p.executeFuncs(funcs)
}

func (p *PIDFile) writePID() error {
	pid := os.Getpid()
	_, err := p.file.WriteString(fmt.Sprintf("%d", pid))
	return err
}

func (p *PIDFile) executeFuncs(funcs []func() error) error {
	for _, fn := range funcs {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

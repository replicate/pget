//go:build !windows

package cli

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/replicate/pget/pkg/logging"
)

type PIDFile struct {
	Path string
	file *os.File
	fd   int
}

func (p *PIDFile) tryCreateLockFile(path string) (*os.File, error) {
	logger := logging.GetLogger()

	lockedFile, err := os.OpenFile(p.Path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)

	if err != nil {
		if errors.Is(err, os.ErrExist) {
			logger.Warn().
				Err(err).
				Str("warn_message", "Another pget process may be running. Use 'pget multifile' to download multiple files in parallel.").
				Msg("Waiting on Lock")
		} else {
			return nil, err
		}

	}
	return lockedFile, nil
}

func (p *PIDFile) acquireLock() error {
	logger := logging.GetLogger()
	var lockedFile *os.File
	var err error
	for {
		logger.Debug().Str("path", p.Path).Msg("Attempting Lock Acquire")
		lockedFile, err = p.tryCreateLockFile(p.Path)
		if err != nil {
			// TODO: consider adding a validation to ensure that the PID in the lock file is still running
			// and if not, remove the lock file and try again
			time.Sleep(100 * time.Millisecond)
			continue
		}
		p.file = lockedFile
		p.fd = int(lockedFile.Fd())
		logger.Debug().Str("path", p.Path).Msg("Lock Acquired")
		return nil
	}
}

func (p *PIDFile) Acquire() error {
	funcs := []func() error{
		p.acquireLock,
		p.writePID,
		p.file.Sync,
	}
	return p.executeFuncs(funcs)
}

func (p *PIDFile) Remove() error {
	err := os.Remove(p.Path)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (p *PIDFile) Release() error {
	funcs := []func() error{
		p.file.Close,
		p.Remove,
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

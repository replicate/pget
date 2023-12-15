//go:build !windows

package cli

import (
	"fmt"
	"os"
	"syscall"
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
	funcs := []func() error{
		func() error { return syscall.Flock(p.fd, syscall.LOCK_EX) },
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

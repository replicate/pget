SHELL := /bin/bash

DESTDIR ?=
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

INSTALL := install -m 0755
INSTALL_PROGRAM := $(INSTALL)

GO := go
GOOS := $(shell $(GO) env GOOS)
GOARCH := $(shell $(GO) env GOARCH)

default: all

.PHONY: all
all: pget

pget:
	CGO_ENABLED=0 $(GO) build -o $@ \
		-ldflags '-extldflags "-static"' \
		main.go

.PHONY: install
install: pget
	$(INSTALL_PROGRAM) -d $(DESTDIR)$(BINDIR)
	$(INSTALL_PROGRAM) pget $(DESTDIR)$(BINDIR)/pget

.PHONY: uninstall
uninstall:
	rm -f $(DESTDIR)$(BINDIR)/pget

.PHONY: clean
clean:
	$(GO) clean
	rm -f replicate

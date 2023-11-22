SHELL := /bin/bash

DESTDIR ?=
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

INSTALL := install -m 0755
INSTALL_PROGRAM := $(INSTALL)

GO := go
GOOS := $(shell $(GO) env GOOS)
GOARCH := $(shell $(GO) env GOARCH)

GIT_TAG := $(shell git describe --tags --abbrev=0 2>/dev/null)
GIT_TAG_COMMIT := $(shell git rev-list -n 1 $(GIT_TAG) 2>/dev/null | cut -c1-7)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
GIT_DIRTY := $(shell git diff --quiet && echo 0 || echo 1)
BUILD_TIME := $(shell date +%Y-%m-%dT%H:%M:%S%z)

ifeq ($(GIT_DIRTY),1)
    VERSION := "development-$(GIT_COMMIT)-uncomitted-changes"
else ifeq ($(strip $(GIT_COMMIT)), $(strip $(GIT_TAG_COMMIT)))
    VERSION := $(GIT_TAG)
else
    VERSION := "development-$(GIT_COMMIT)"
endif

LD_FLAGS := -ldflags "-extldflags '-static' -X github.com/replicate/pget/pkg/version.Version=$(VERSION) -X github.com/replicate/pget/pkg/version.CommitHash=$(GIT_COMMIT) -X github.com/replicate/pget/pkg/version.BuildTime=$(BUILD_TIME) -w"

default: all

.PHONY: all
all: clean pget

pget:
	CGO_ENABLED=0 $(GO) build -o $@ \
		$(LD_FLAGS) \
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

.PHONY: test
test:
	$(GO) test ./...

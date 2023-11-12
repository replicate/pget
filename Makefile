SHELL := /bin/bash

DESTDIR ?=
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

INSTALL := install -m 0755
INSTALL_PROGRAM := $(INSTALL)

GO := go
GOOS := $(shell $(GO) env GOOS)
GOARCH := $(shell $(GO) env GOARCH)

APP_NAME := pget

GIT_TAG := $(shell git describe --tags --abbrev=0 2>/dev/null)
GIT_TAG_COMMIT := $(shell git rev-list -n 1 $(GIT_TAG) 2>/dev/null | cut -c1-7)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
GIT_DIRTY := $(shell git diff --quiet && echo 0 || echo 1)
BUILD_TIME := $(shell date +%Y-%m-%dT%H:%M:%S%z)


ifeq ($(strip $(GIT_COMMIT)), $(strip $(GIT_TAG_COMMIT)))
    VERSION := $(GIT_TAG)
else ifeq ($(GIT_DIRTY),1)
    VERSION := "dev"
else
    VERSION := $(GIT_COMMIT)
endif

LD_FLAGS := -ldflags "-extldflags '-static' -X github.com/replicate/pget/version.Version=$(VERSION) -X github.com/replicate/pget/version.CommitHash=$(GIT_COMMIT) -X github.com/replicate/pget/version.BuildTime=$(BUILD_TIME) -w"

default: build

.PHONY: build
build: $(APP_NAME)

$(APP_NAME):
	CGO_ENABLED=0 $(GO) build -o $@ $(LD_FLAGS) main.go

.PHONY: install
install: build
	$(INSTALL_PROGRAM) -d $(DESTDIR)$(BINDIR)
	$(INSTALL_PROGRAM) $(APP_NAME) $(DESTDIR)$(BINDIR)/$(APP_NAME)

.PHONY: uninstall
uninstall:
	rm -f $(DESTDIR)$(BINDIR)/$(APP_NAME)

.PHONY: clean
clean:
	$(GO) clean
	rm -f $(APP_NAME)

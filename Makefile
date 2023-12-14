SHELL := /bin/bash

DESTDIR ?=
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

INSTALL := install -m 0755
INSTALL_PROGRAM := $(INSTALL)

CHECKSUM_CMD := shasum -a 256
CHECKSUM_FILE := sha256sum.txt

GO := go
GOOS := $(shell $(GO) env GOOS)
GOARCH := $(shell $(GO) env GOARCH)
GORELEASER := goreleaser

SINGLE_TARGET=--single-target

default: all

.PHONY: all
all: clean build

.PHONY: install
install: build
	$(INSTALL_PROGRAM) -d $(DESTDIR)$(BINDIR)
	$(INSTALL_PROGRAM) pget $(DESTDIR)$(BINDIR)/pget

.PHONY: uninstall
uninstall:
	rm -f $(DESTDIR)$(BINDIR)/pget

.PHONY: clean
clean:
	$(GO) clean
	rm -rf dist
	rm -f pget


.PHONY: test-all
test-all: test lint

.PHONY: test
test:
	script/test $(ARGS)

.PHONY: lint
lint: CHECKONLY=1
lint: format
	script/lint

.PHONY: format
format: CHECKONLY=1
format:
	CHECKONLY=$(CHECKONLY) script/format

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: check-goreleaser
check-goreleaser:
	@command -v goreleaser >/dev/null 2>&1 || { echo >&2 "goreleaser is required but not installed. Aborting. Run 'make install-goreleaser' to install"; exit 1; }

.PHONY: install-goreleaser
install-goreleaser:
	@command -v goreleaser >/dev/null 2>&1 || { \
		echo >&2 "goreleaser is required but not installed. Installing..."; \
		curl -sfL https://install.goreleaser.com/github.com/goreleaser/goreleaser.sh | sh; \
	}

.PHONY: build
build: pget

.PHONY: build-all
build-all: SINGLE_TARGET:=
build-all: clean pget

pget: check-goreleaser
	$(GORELEASER) build --snapshot --rm-dist $(SINGLE_TARGET) -o ./pget

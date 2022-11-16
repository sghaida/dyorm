# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=  GOMAXPROCS=2 $(GOCMD) test -p 5
GOGET=$(GOCMD) get
GORUN=$(GOCMD) run

.PHONY: init
init:
	$(GOCMD) mod download

.PHONY: test
test: clean-test
	$(GOTEST) --tags=!integration   ./...

.PHONY: test-race
test-race: clean-test
	$(GOTEST) --tags=!integration --race ./...

.PHONY: clean-test
clean-test:
	$(GOCLEAN) -testcache

.PHONY: vet
vet:
	$(GOCMD) vet ./...

.PHONY: coverage-badge
coverage-badge:
	$(GOTEST) -race -coverprofile=coverage.txt -covermode=atomic ./...


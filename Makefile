.PHONY: all
all: fmt build test

.PHONY: build
build:
	go build ./...

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: tets 
test:
	go test -race ./... 

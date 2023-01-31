.PHONY: all
all: build test

.PHONY: build
build:
	go build ./...

.PHONY: tets 
test:
	go test -race ./... 

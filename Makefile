PKG=$(shell go list ./... | grep -v vendor)

default: vet test

vet:
	go vet $(PKG)

test:
	go test $(PKG)

deps:
	dep ensure -v

.PHONY: vet test deps

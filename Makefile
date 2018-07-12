default: vet test

vet:
	go vet ./...

test:
	go test ./...

deps:
	dep ensure -v

errcheck:
	errcheck ./...

.PHONY: vet test deps errcheck

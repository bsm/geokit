default: vet test

vet:
	go vet ./...

test:
	go test ./...

bench:
	go test ./... -run=NONE -bench=. -benchmem

deps:
	dep ensure -v

errcheck:
	errcheck ./...

.PHONY: vet test deps errcheck

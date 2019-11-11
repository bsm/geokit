default: vet test

vet/%: %
	@cd $< && go vet ./...

test/%: %
	@cd $< && go test ./...

bench/%: %
	@cd $< && go test ./... -run=NONE -bench=. -benchmem

staticcheck/%: %
	@cd $< && staticcheck ./...

update-deps/%: %
	@cd $< && go get -u ./... && go mod tidy

vet: $(patsubst %/go.mod,vet/%,$(wildcard */go.mod))
test: $(patsubst %/go.mod,test/%,$(wildcard */go.mod))
bench: $(patsubst %/go.mod,bench/%,$(wildcard */go.mod))
staticcheck: $(patsubst %/go.mod,staticcheck/%,$(wildcard */go.mod))
update-deps: $(patsubst %/go.mod,update-deps/%,$(wildcard */go.mod))

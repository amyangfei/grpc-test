.PHONY: gen-pb

build:
	go build -o bin/server server/main.go
	go build -o bin/client client/main.go

protoc-gen-gogofaster:
	go build -o bin/$@ github.com/gogo/protobuf/protoc-gen-gogofaster


goimports:
	go build -o bin/$@ golang.org/x/tools/cmd/goimports

gen-bin: protoc-gen-gogofaster goimports
	./generate-proto.sh



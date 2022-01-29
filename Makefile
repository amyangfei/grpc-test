.PHONY: gen-pb

build:
	go build -o bin/server server/main.go
	go build -o bin/client client/main.go
	go build -o bin/feed_server cmd/feed_server_main.go
	go build -o bin/feed_client cmd/feed_client_main.go

protoc-gen-gogofaster:
	go build -o bin/$@ github.com/gogo/protobuf/protoc-gen-gogofaster


goimports:
	go build -o bin/$@ golang.org/x/tools/cmd/goimports

gen-bin: protoc-gen-gogofaster goimports
	./generate-proto.sh



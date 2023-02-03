.PHONY: gen-pb

build:
	go build -o bin/server server/main.go
	go build -o bin/client client/main.go
	go build -o bin/feed_server cmd/feed_server_main.go
	go build -o bin/feed_client cmd/feed_client_main.go
	go build -o bin/elastic_mysql_client cmd/elastic_mysql_main.go
	go build -o bin/mysql_updater_client cmd/mysql_update_op/main.go

protoc-gen-gogofaster:
	go build -o bin/$@ github.com/gogo/protobuf/protoc-gen-gogofaster


goimports:
	go build -o bin/$@ golang.org/x/tools/cmd/goimports

gen-bin: protoc-gen-gogofaster goimports
	./generate-proto.sh



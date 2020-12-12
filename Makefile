.PHONY: gen-pb

build:
	go build -o bin/server server/main.go
	go build -o bin/client client/main.go

gen-pb:
	cd ./proto && protoc -I. message.proto --gofast_out=plugins=grpc:../pb

#!/usr/bin/env bash

# If the environment variable is unset, GOPATH defaults
# to a subdirectory named "go" in the user's home directory
# ($HOME/go on Unix, %USERPROFILE%\go on Windows),
# unless that directory holds a Go distribution.
# Run "go env GOPATH" to see the current GOPATH.
GOPATH=$(go env GOPATH)
echo "using GOPATH=$GOPATH"

BIN_DIR=$(pwd)/bin

case "$(uname)" in
MINGW*)
	EXE=.exe
	;;
*)
	EXE=
	;;
esac

# use `protoc-gen-gogofaster` rather than `protoc-gen-go`.
GOGO_FASTER=$BIN_DIR/protoc-gen-gogofaster$EXE
if [ ! -f ${GOGO_FASTER} ]; then
	echo "${GOGO_FASTER} does not exist, please 'run make protoc-gen-gogofaster' first"
	exit 1
fi

# get and construct the path to gogo/protobuf.
GO111MODULE=on go get -d github.com/gogo/protobuf
GOGO_MOD=$(GO111MODULE=on go list -m github.com/gogo/protobuf)
GOGO_PATH=$GOPATH/pkg/mod/${GOGO_MOD// /@}
if [ ! -d ${GOGO_PATH} ]; then
	echo "${GOGO_PATH} does not exist, please ensure 'github.com/gogo/protobuf' is in go.mod"
	exit 1
fi

cd ./proto || exit 1

echo "generate protobuf code..."

protoc -I. -I"${GOGO_PATH}" -I"${GOGO_PATH}/protobuf" --plugin=protoc-gen-gogofaster=${GOGO_FASTER} --gogofaster_out=plugins=grpc:../pb/ *.proto

cd ../pb || exit 1
sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
sed -i.bak -E 's/import fmt \"fmt\"//g' *.pb.go
rm -f *.bak
$BIN_DIR/goimports -w *.pb.go

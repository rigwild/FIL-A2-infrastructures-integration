#!/bin/bash

# Check the package name was passed
if [ $# -eq 0 ]; then
  echo "You need to pass the package name (e.g. \"$0 my-package\" where \"my-package\" is at cmd/my-package"
  exit 1
fi

PROJECT_ROOT=$(realpath ${BASH_SOURCE%/*}/..)
OUTPUT_DIR=${PROJECT_ROOT}/bin
CMD_DIR=${PROJECT_ROOT}/cmd
CMD_TO_BUILD=$1
CMD_TO_BUILD_DIR=$CMD_DIR/$CMD_TO_BUILD

# Check the package directory exists in cmd/my-package
if [ ! -d "$CMD_TO_BUILD_DIR" ]; then
  echo "The package \"$CMD_TO_BUILD\" was not found at cmd/$CMD_TO_BUILD"
  exit 1
fi

cd $PROJECT_ROOT
echo "Building the \"$CMD_TO_BUILD\" package binary to $OUTPUT_DIR"
mkdir -p $OUTPUT_DIR
time go build -o $OUTPUT_DIR "$CMD_TO_BUILD_DIR/..."

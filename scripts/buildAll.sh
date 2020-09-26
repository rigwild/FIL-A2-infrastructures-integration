#!/bin/bash

PROJECT_ROOT=$(realpath ${BASH_SOURCE%/*}/..)
OUTPUT_DIR=${PROJECT_ROOT}/bin
CMD_DIR=${PROJECT_ROOT}/cmd

cd $PROJECT_ROOT
echo "Building all packages binaries to $OUTPUT_DIR"
mkdir -p $OUTPUT_DIR
time go build -o $OUTPUT_DIR "$CMD_DIR/..."

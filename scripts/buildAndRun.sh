#!/bin/bash

source ${BASH_SOURCE%/*}/build.sh \
  && echo -e "\n\nRunning the \"$CMD_TO_BUILD\" package binary.\n-----\n" \
  && $OUTPUT_DIR/$CMD_TO_BUILD

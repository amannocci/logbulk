#!/bin/bash

# Correct path
cd "$(dirname "$0")"
BASE_DIR=$PWD
BASE_PROJECT=$(dirname "$BASE_DIR")
BASE_EXAMPLE=${BASE_DIR}/$1

# Load common
source ${BASE_PROJECT}/build/common.sh
info "Loading common"

# Check needed
if [ "$(is_install docker)" == "0" ]
then
  error "Please install docker to continue"
  exit 1
fi

# Extract version
cd ${BASE_PROJECT}
get_version

docker run --rm --name logbulk-${VERSION} --net=host \
    -e "-Dconfig.file=conf/app.conf" \
    -v ${BASE_EXAMPLE}:/usr/logbulk/conf \
    amannocci/logbulk:${VERSION}
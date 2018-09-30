#!/bin/bash

# Correct path
cd "$(dirname "$0")"
BASE_DIR=$PWD
BASE_PROJECT=$(dirname "$BASE_DIR")

# Load common
source ${BASE_DIR}/common.sh
info "Loading common"

# Check needed
if [ "$(is_install mvn)" == "1" ]
then
  error "Please install maven to continue"
  exit 1
fi
if [ "$(is_install docker)" == "1" ]
then
  error "Please install docker to continue"
  exit 1
fi

# Extract version
cd ${BASE_PROJECT}
get_version

# Build logbulk before anything
mvn clean install package

# Build all plugins
${BASE_DIR}/build-plugins.sh

# Build logbulk
docker build -t amannocci/logbulk:${VERSION} .
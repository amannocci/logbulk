#!/bin/bash

# Correct path
cd "$(dirname "$0")"
BASE_DIR=$PWD
BASE_PROJECT=$(dirname "$BASE_DIR")
BASE_DEV=$(dirname "$BASE_PROJECT")

# Load common
source ${BASE_DIR}/common.sh
info "Loading common"

# Check needed
if [ "$(is_install git)" == "1" ]
then
  error "Please install git to continue"
  exit 1
fi

# Plugins list
source ${BASE_DIR}/list-plugins.sh
info "Loading plugins list"

# Retrieve plugins from list
for plugin in ${plugins[@]};
do
  cd ${BASE_DEV}
  if [ -d "./logbulk-plugin-${plugin}" ]; then
    info "Update plugin : ${plugin}"
    cd ./logbulk-plugin-${plugin}
    git pull
  fi
done
exit 0

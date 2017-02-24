#!/bin/bash

# Correct path
cd "$(dirname "$0")"
BASE_DIR=$PWD
BASE_PROJECT=$(dirname "$BASE_DIR")

# Load common
source ${BASE_DIR}/common.sh
info "Loading common"

# Check needed
if [ "$(is_install git)" == "1" ]
then
  error "Please install git to continue"
  exit 1
fi
if [ "$(is_install mvn)" == "1" ]
then
  error "Please install maven to continue"
  exit 1
fi

# Plugins list
declare -a plugins=(
  "std"
  "file"
  "anonymise"
)

# Create plugins dir
if [ -d "${BASE_PROJECT}/plugins" ]; then
  info "Clean old plugins directory"
  rm -rf ${BASE_PROJECT}/plugins
fi
info "Create plugins directory"
mkdir ${BASE_PROJECT}/plugins

# Retrieve plugins from list
for plugin in ${plugins[@]};
do
  info "Attempt to import plugin: ${plugin}"
  cd ${BASE_PROJECT}
  if [ -d "./plugins/logbulk-plugin-${plugin}" ]; then
    info "Removing old plugin directory"
    rm -rf ./plugins/logbulk-plugin-${plugin}
  fi
  info "Clone plugin directory"
  git clone https://github.com/amannocci/logbulk-plugin-${plugin} ./plugins/logbulk-plugin-${plugin}
  cd ./plugins/logbulk-plugin-${plugin}
  info "Compile ${plugin} plugin"
  mvn clean package

  if [ "$?" == "0" ]
  then
    cp target/logbulk-plugin-*.jar ${BASE_PROJECT}/plugins
    info "Succesfully import plugin ${plugin}"
  else
    error "Fail to compile plugin ${plugin}"
  fi
  rm -rf ${BASE_PROJECT}/plugins/logbulk-plugin-${plugin}
done
exit 0
#!/usr/bin/env bash

# Correct path
cd "$(dirname "$0")"
BASE_DIR=$PWD
LOGBULK_LIB="${BASE_DIR}/lib"

# Settings
MEM="1G"
CONF=""

# Classpath
if [ "$(uname -o)" == "Cygwin" ]
then
  DELIMITER=${IFS}
  TMP=$(find "$LOGBULK_LIB" -name '*.jar' -printf '%p|')
  export IFS="|"
  CLASSPATH=""

  for dependence in ${TMP}; do
    CLASSPATH+="$(cygpath -w ${dependence});"
  done
  CLASSPATH=$(echo ${CLASSPATH} | sed 's/;$//')
  export IFS=${DELIMITER}
else
  CLASSPATH=$(find "$LOGBULK_LIB" -name '*.jar' -printf '%p:' | sed 's/:$//')
fi

# Launch
exec java -XX:+UseG1GC -Xmx${MEM} \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=conf/heapdump.hprof \
  -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
  -Dconfig.file=conf/application.conf ${CONF} \
  -server -cp ${CLASSPATH} io.vertx.core.Launcher --nodaemon
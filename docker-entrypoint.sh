#!/usr/bin/env bash
# Classpath
CLASSPATH=$(find "$LOGBULK_LIB" -name '*.jar' -printf '%p:' | sed 's/:$//')

# Launch
exec java -XX:+UseG1GC -Xmx${MEM} \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=conf/heapdump.hprof \
  -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
  -Dconfig.file=conf/app.conf ${CONF} \
  -server -cp ${CLASSPATH} io.vertx.core.Launcher --nodaemon
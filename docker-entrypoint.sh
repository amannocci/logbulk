#!/usr/bin/env bash
# Classpath
CLASSPATH=$(find "$LOGBULK_LIB" -name '*.jar' -printf '%p:' | sed 's/:$//')

# Launch
java -Xmx${MEM} -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory -Dconfig.file=conf/app.conf -server -cp ${CLASSPATH} io.vertx.core.Launcher
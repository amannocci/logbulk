FROM openjdk:8u102-jdk

# Maintainer
MAINTAINER adrien.mannocci@gmail.com

# Logbulk version
ENV LOGBULK_VERSION 0.1.0-dev
ENV LOGBULK_FILE logbulk-$LOGBULK_VERSION-fat.jar

# Set some env variables
ENV LOGBULK_HOME /usr/logbulk
ENV LOGBULK_LIB $LOGBULK_HOME/lib
ENV MEM 2G
ENV CONF ""

# Create directory
RUN mkdir -p $LOGBULK_LIB

# Volumes
VOLUME ["/usr/logbulk/conf", "/usr/logbulk/log", "/usr/logbulk/lib"]

# Copy logbulk fat jar to the container
COPY target/$LOGBULK_FILE $LOGBULK_LIB/$LOGBULK_FILE
COPY ./docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

# Launch logbulk
WORKDIR $LOGBULK_HOME
ENTRYPOINT ["/docker-entrypoint.sh"]
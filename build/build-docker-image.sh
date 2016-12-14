#!/usr/bin/env bash
VERSION=$(grep -oP "<version>(.*)</version>" pom.xml | head -1 | sed 's/\(<version>\|<\/version>\)//g')
mvn clean package
docker build -t amannocci/logbulk:$VERSION
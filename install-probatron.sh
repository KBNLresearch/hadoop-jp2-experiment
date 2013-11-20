#!/bin/bash

mvn install:install-file -Dfile=src/main/resources/external-tools/probatron.jar -DgroupId=org.probatron \
    -DartifactId=probatron -Dversion=0.7.4 -Dpackaging=jar

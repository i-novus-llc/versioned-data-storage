#!/bin/bash

printenv | grep TRAVIS_TAG; if [[ $TRAVIS_TAG == '' ]] ; then
  mvn --settings .maven.xml clean install -B -U
else
  mvn --settings .maven.xml clean deploy -DskipTests=true -Prelease -B -U
fi
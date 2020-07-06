#!/bin/bash

printenv | grep TRAVIS_TAG; if [[ $TRAVIS_TAG == '' ]] ; then
  echo "TAG: $TRAVIS_TAG"
  mvn --settings .maven.xml clean install -B -U
else
  echo "TAG: $TRAVIS_TAG"
  mvn --settings .maven.xml clean deploy -DskipTests=true -Prelease -B -U
fi
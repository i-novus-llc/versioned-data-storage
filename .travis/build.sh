#!/bin/bash

if [ $TRAVIS_BRANCH == 'master' ] || [ $TRAVIS_BRANCH == 'develop' ] ; then
  mvn --settings .maven.xml clean install -B -U
else
  mvn --settings .maven.xml clean deploy -DskipTests=true -Prelease -B -U
fi
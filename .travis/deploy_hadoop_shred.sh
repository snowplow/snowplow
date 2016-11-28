#!/bin/bash

tag=$1

cicd=${tag:0:13}
release=${tag:13}

if [ "${cicd}" == "hadoop_shred/" ]; then
    if [ "${release}" == "" ]; then
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
else
    echo "This can't be deployed - there's no hadoop_shred tag! (is the travis condition set?)"
    exit 1
fi
 
cd $TRAVIS_BUILD_DIR

export TRAVIS_BUILD_RELEASE_TAG=${release}
release-manager --config ./.travis/release_hadoop_shred.yml --check-version --make-artifact --upload-artifact

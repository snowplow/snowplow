#!/bin/bash

tag=$1

cicd=${tag:0:14}
release=${tag:14}

if [ "${cicd}" == "hadoop_enrich/" ]; then
    if [ "${release}" == "" ]; then
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
else
    echo "This can't be deployed - there's no hadoop_enrich tag! (is the travis condition set?)"
    exit 1
fi
 
cd $TRAVIS_BUILD_DIR

export TRAVIS_BUILD_RELEASE_TAG=${release}
release-manager --config ./.travis/release_hadoop_enrich.yml --check-version --make-artifact --upload-artifact

#!/bin/bash

tag=$1

cicd=${tag:0:4}
release=${tag:4}

if [ "${cicd}" == "emr/" ]; then
    if [ "${release}" == "" ]; then
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
else
    echo "This can't be deployed - there's no emr tag! (is the travis condition set?)"
    exit 1
fi
 
cd $TRAVIS_BUILD_DIR

export TRAVIS_BUILD_RELEASE_TAG=${release}
release-manager --config ./.travis/release_emr.yml --check-version --make-version --make-artifact --upload-artifact

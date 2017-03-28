#!/bin/bash

tag=$1

cicd=${tag:0:25}
release=${tag:25}

if [ "${cicd}" == "event_manifest_populator/" ]; then
    if [ "${release}" == "" ]; then
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
else
    echo "This can't be deployed - there's no event_manifest_populator tag! (is the travis condition set?)"
    exit 1
fi
 
cd $TRAVIS_BUILD_DIR

export TRAVIS_BUILD_RELEASE_TAG=${release}
release-manager --config ./.travis/release_event_manifest_populator.yml --check-version --make-artifact --upload-artifact

#!/bin/bash

tag=$1

cicd=${tag:0:8}
release=${tag:8}

if [ "${cicd}" == "kinesis/" ]; then
    if [ "${release}" == "" ]; then
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
else
    echo "This can't be deployed - there's no kinesis tag! (is the travis condition set?)"
    exit 1
fi
 
cd $TRAVIS_BUILD_DIR
./ci.bash "${release}"

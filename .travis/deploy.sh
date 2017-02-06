#!/bin/bash

project=$1
tag=$2

slashed="${project}/"
slashed_len=${#slashed}

cicd=${tag:0:${slashed_len}}
release=${tag:${slashed_len}}

if [ "${cicd}" == "${slashed}" ]; then
    if [ "${release}" == "" ]; then
        echo "WARNING! No release specified! Ignoring."
        exit 2
    fi
else
    echo "This can't be deployed - there's no ${project} tag! (Is the travis condition set?)"
    exit 1
fi

cd "${TRAVIS_BUILD_DIR}"

export TRAVIS_BUILD_RELEASE_TAG="${release}"
release-manager \
    --config "./.travis/release_${project}.yml" \
    --check-version \
    --make-version \
    --make-artifact \
    --upload-artifact

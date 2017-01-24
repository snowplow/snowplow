#!/bin/bash

tag=$1

project_name="relational_database_shredder/"
project_name_len=${#project_name}

cicd=${tag:0:${project_name_len}}
release=${tag:${project_name_len}}

if [ "${cicd}" == "${project_name}" ]; then
    if [ "${release}" == "" ]; then
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
else
    echo "This can't be deployed - there's no relational_database_shredder tag! (is the travis condition set?)"
    exit 1
fi
 
cd $TRAVIS_BUILD_DIR

export TRAVIS_BUILD_RELEASE_TAG=${release}
release-manager --config ./.travis/release_relational_database_shredder.yml --check-version --make-artifact --upload-artifact

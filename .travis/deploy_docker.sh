#!/bin/bash

tag=$1

file="${HOME}/.dockercfg"
docker_repo="snowplow-docker-registry.bintray.io"
curl -X GET \
    -u${BINTRAY_SNOWPLOW_DOCKER_USER}:${BINTRAY_SNOWPLOW_DOCKER_API_KEY} \
    https://${docker_repo}/v2/auth > $file

cd $TEST_DIR

project_version=$(sbt version -Dsbt.log.noformat=true | perl -ne 'print "$1\n" if /info.*(\d+\.\d+\.\d+[^\r\n]*)/' | head -n 1 | tr -d '\n')
if [ "${project_version}" == "${tag}" ]; then
    sbt docker:publish
else
    echo "Tag version '${tag_version}' doesn't match version in scala project ('${project_version}'). Aborting!"
    exit 1
fi


#!/bin/bash

tag=$1

file="${HOME}/.dockercfg"
docker_repo="snowplow-docker-registry.bintray.io"
docker login -u ${BINTRAY_SNOWPLOW_DOCKER_USER} -p ${BINTRAY_SNOWPLOW_DOCKER_API_KEY} ${docker_repo}

cd $TEST_DIR

project_version=$(sbt version -Dsbt.log.noformat=true | perl -ne 'print "$1\n" if /info.*(\d+\.\d+\.\d+[^\r\n]*)/' | tail -n 1 | tr -d '\n')
if [[ "${tag}" = *"${project_version}" ]]; then
    sbt docker:publishLocal
    formatted_tag="${tag////:}"
    docker_push_url="${docker_repo}/snowplow/${formatted_tag//_/-}"
    echo "Pushing ${docker_push_url}"
    docker push ${docker_push_url}
else
    echo "Tag version '${tag}' doesn't match version in scala project ('${project_version}'). Aborting!"
    exit 1
fi

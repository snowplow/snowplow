#!/bin/bash

tag_version=$1

mkdir ~/.bintray/
file=$HOME/.bintray/.credentials
cat <<EOF >$file
realm = Bintray API Realm
host = api.bintray.com
user = $BINTRAY_SNOWPLOW_MAVEN_USER
password = $BINTRAY_SNOWPLOW_MAVEN_API_KEY
EOF

cd $TRAVIS_BUILD_DIR

project_version=$(sbt version -Dsbt.log.noformat=true | perl -ne 'print "$1\n" if /(\d+\.\d+\.\d+[^\r\n]*)/' | head -n 1 | tr -d '\n')
if [ "${project_version}" == "${tag_version}" ]; then
    sbt +publish
    sbt +bintraySyncMavenCentral
else
    echo "Tag version '${tag_version}' doesn't match version in scala project ('${project_version}'). Aborting!"
    exit 1
fi

#!/bin/sh

# Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::        Fred Blundun (mailto:support@snowplowanalytics.com)
# Copyright::     Copyright (c) 2016 Snowplow Analytics Ltd
# License::       Apache License Version 2.0

# Version::       0.2.0
# Compatibility:: AMI 4.1.0

# Fast fail
set -e

# Get commons-codec version as optional first argument
if [ -z $1 ]; then CC_VERSION="1.5"; else CC_VERSION=$1; fi

# Because AMI 4.1.0 has commons-codecs which are binary
# incompatible with shipped with Scala Hadoop Enrich,
# we download appropriate version to the classpath.
# See https://forums.aws.amazon.com/thread.jspa?threadID=173258
function fix_commons_codec() {
    wget "http://central.maven.org/maven2/commons-codec/commons-codec/$CC_VERSION/commons-codec-$CC_VERSION.jar"
    sudo mkdir -p /usr/lib/hadoop/lib
    sudo cp "commons-codec-$CC_VERSION.jar" "/usr/lib/hadoop/lib/remedial-commons-codec-$CC_VERSION.jar"
    rm "commons-codec-$CC_VERSION.jar"
    echo "Fixed commons-codec-$CC_VERSION.jar"
}

# Because the AMIs have Scala 2.11 installed, and this
# is binary incompatible with Snowplow's bundled Scala 2.10.
function delete_scala_211() {
    sudo find / -name "scala-library-2.11.*.jar" -exec rm -rf {} \;
    sudo rm -rf /usr/share/scala/lib
    echo "Deleted Scala 2.11"
}

# Run bootstrap actions
delete_scala_211
fix_commons_codec

# Remove commons-codec-1.4.jar if it appears
while true; do
    sleep 5;
    sudo rm /usr/lib/hadoop/lib/commons-codec-1.4.jar && break || true;
done &

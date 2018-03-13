#!/bin/sh

# Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::        Anton Parkhomenko (mailto:support@snowplowanalytics.com)
# Copyright::     Copyright (c) 2012-2018 Snowplow Analytics Ltd
# License::       Apache License Version 2.0

# Version::       0.1.0-rc1
# Compatibility:: AMI 5.0.0

# Fast fail
set -e

# Get commons-codec version as optional first argument
if [ -z $1 ]; then CC_VERSION="1.5"; else CC_VERSION=$1; fi

# Because AMIs from 3.x up to 5.x have commons-codecs which are binary
# incompatible with shipped with Spark Enrich,
# we download appropriate version to the classpath.
# See https://forums.aws.amazon.com/thread.jspa?threadID=173258
function fix_commons_codec() {
    wget "http://central.maven.org/maven2/commons-codec/commons-codec/$CC_VERSION/commons-codec-$CC_VERSION.jar"
    sudo mkdir -p /usr/lib/hadoop/lib
    sudo cp "commons-codec-$CC_VERSION.jar" "/usr/lib/hadoop/lib/remedial-commons-codec-$CC_VERSION.jar"
    rm "commons-codec-$CC_VERSION.jar"
    echo "Fixed commons-codec-$CC_VERSION.jar"
}

# Run bootstrap actions
fix_commons_codec

# Fix "reset by peer" crashes of RDB Loader when EMR is running behind NAT Gateway and load takes more than 10 mins
sudo /sbin/sysctl -w net.ipv4.tcp_keepalive_time=200 net.ipv4.tcp_keepalive_intvl=200 net.ipv4.tcp_keepalive_probes=5

# Remove commons-codec-1.4.jar if it appears
while true; do
    sleep 5;
    sudo rm /usr/lib/hadoop/lib/commons-codec-1.4.jar && echo 'Deleted!' && break || true;
done &

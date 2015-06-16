#!/bin/sh

# Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::        Alex Dean (mailto:support@snowplowanalytics.com)
# Copyright::     Copyright (c) 2015 Snowplow Analytics Ltd
# License::       Apache License Version 2.0

# Version::       0.1.0
# Compatibility:: AMI 3.6.0, 3.7.0

# Because the 3.x AMIs have old commons-codecs which are binary
# incompatible with 1.5+.
# See https://forums.aws.amazon.com/thread.jspa?threadID=173258
function delete_commons_codec() {
	sudo find / -name "commons-codec-1.2.jar" -exec rm -rf {} \;
	sudo find / -name "commons-codec-1.3.jar" -exec rm -rf {} \;
	sudo find / -name "commons-codec-1.4.jar" -exec rm -rf {} \;
}

# Because the 3.x AMIs have Scala 2.11 installed, and this
# is binary incompatible with Snowplow's bundled Scala 2.10.
function delete_scala_211() {
	sudo find / -name "scala-library-2.11.*.jar" -exec rm -rf {} \;
	sudo rm -rf /usr/share/scala/lib
}


# Run bootstrap actions

delete_commons_codec
delete_scala_211

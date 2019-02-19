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

# Author::    Joshua Beemster (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2018 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

# Recursively removes a list of directories on HDFS
for i in "${@}"
do
    hadoop fs -test -d ${i}
    if [ $? == 0 ]; then
        echo "Removing directory ${i} ..."
        hadoop fs -rm -r -skipTrash ${i}
    else
        echo "Directory ${i} does not exist"
    fi
done

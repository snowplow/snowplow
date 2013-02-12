#!/bin/bash
# Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Alex Dean (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2013 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

# input parameters
if [ $# != 2 ]; then
    echo; echo 'syntax: '$0' <username> <password>'; echo
    exit 1
else
    # assign parameters
    USERNAME=$1
    PASSWORD=$2
fi

SQL=`locate mysql-ib`

echo "Creating new Infobright table 0.0.5"
cat ./sql/setup_infobright.sql | ${SQL} -u ${USERNAME} --password=${PASSWORD}
echo "... done."

echo "Updating Infobright table from 0.0.4 to 0.0.5..."
cat ./sql/migrate_0.0.4_to_0.0.5.sql | ${SQL} -u ${USERNAME} --password=${PASSWORD}
echo "... done."

echo "Showing the new table definition..."
cat ./sql/verify_infobright.sql | ${SQL} -u ${USERNAME} --password=${PASSWORD}
echo "... done."

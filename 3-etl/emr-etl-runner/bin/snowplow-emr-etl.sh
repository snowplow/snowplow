#!/bin/bash

# Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# See also: 4-storage/storage-loader/bin/snowplow-runner-and-loader
#           (runs both the EmrEtlRunner and the StorageLoader)

# Update these for your environment
EMR_RUNNER_PATH=/path/to/snowplow/3-etl/snowplow-emr-etl-runner
CONFIG_FILE=/path/to/your-config.yml

# Run the daily ETL job
BUNDLE_GEMFILE=${EMR_RUNNER_PATH}
bundle exec snowplow-emr-etl-runner --config ${CONFIG_FILE}

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

# See also: 4-storage/storage-loader/bin/snowplow-runner-and-loader
#           (runs both the EmrEtlRunner and the StorageLoader)

# Update these for your environment
RVM_PATH=/path/to/.rvm # Check the bottom of your .bashrc or .zshrc
BUNDLE_PATH=/path/to/your/bundle-binary # `which bundle` to find this
RUNNER_PATH=/path/to/snowplow/3-etl/snowplow-emr-etl-runner
RUNNER_CONFIG=/path/to/your-config.yml

# Load the RVM environment
export PATH=$PATH:${RVM_PATH}/bin
source ${RVM_PATH}/scripts/rvm

# Run the ETL job on EMR
export BUNDLE_GEMFILE=${RUNNER_PATH}
${BUNDLE_PATH}/bundle exec ${RUNNER_PATH}/bin/snowplow-emr-etl-runner --config ${RUNNER_CONFIG}

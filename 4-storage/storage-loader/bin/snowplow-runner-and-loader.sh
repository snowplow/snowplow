#!/bin/bash

# Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Update these for your environment
rvm_path=/path/to/.rvm # Typically in the $HOME of the user who installed RVM
RUNNER_PATH=/path/to/snowplow/3-enrich/emr-etl-runner
LOADER_PATH=/path/to/snowplow/4-storage/storage-loader
RUNNER_CONFIG=/path/to/your-runner-config.yml
RUNNER_ENRICHMENTS=/path/to/your/enrichment-jsons
LOADER_CONFIG=/path/to/your-loader-config.yml

# Load the RVM environment
export PATH=$PATH:${rvm_path}/bin
source ${rvm_path}/scripts/rvm

# Run the ETL job on EMR
export BUNDLE_GEMFILE=${RUNNER_PATH}/Gemfile
bundle exec ${RUNNER_PATH}/bin/snowplow-emr-etl-runner --config ${RUNNER_CONFIG} --enrichments ${RUNNER_ENRICHMENTS}

# Check the damage
ret_val=$?
if [ $ret_val -ne 0 ]; then
    echo "Error running EmrEtlRunner, exiting with return code ${ret_val}. StorageLoader not run"
    exit $ret_val
fi

# If all okay, run the storage load too
export BUNDLE_GEMFILE=${LOADER_PATH}/Gemfile
bundle exec ${LOADER_PATH}/bin/snowplow-storage-loader --config ${LOADER_CONFIG}

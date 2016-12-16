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
RUNNER_PATH=/path/to/snowplow-emr-etl-runner
LOADER_PATH=/path/to/snowplow-storage-loader
RUNNER_CONFIG=/path/to/your-runner-config.yml
RESOLVER=/path/to/your-resolver.json
RUNNER_ENRICHMENTS=/path/to/your/enrichment-jsons
LOADER_CONFIG=/path/to/your-loader-config.yml
TARGETS_PATH=/path/to/your/targets-jsons

# Run the ETL job on EMR
./${RUNNER_PATH} --config ${RUNNER_CONFIG} --resolver ${RESOLVER} --enrichments ${RUNNER_ENRICHMENTS} --targets ${TARGETS_PATH}

# Check the damage
ret_val=$?
if [ $ret_val -eq 3 ]
then
	echo "No Snowplow logs to process since last run, exiting with return code 0. StorageLoader not run"
	exit 0
elif [ $ret_val -ne 0 ]
then
    echo "Error running EmrEtlRunner, exiting with return code ${ret_val}. StorageLoader not run"
    exit $ret_val
fi

# If all okay, run the storage load too
./${LOADER_PATH} --config ${LOADER_CONFIG} --targets ${TARGETS_PATH} --resolver ${RESOLVER}


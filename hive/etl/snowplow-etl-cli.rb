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

# This Ruby script runs the daily ETL (extract, transform, load)
# process which transforms the raw CloudFront log data into
# SnowPlow-formatted Hive data tables, optimised for analysis.
#
# This is a three-step process:
# 1. Extract the CloudFront log files to a temporary SnowPlow event data table (using the custom Deserializer)
# 2. Load the temporary event data into the final SnowPlow data table, partitioning by date and user
# 3. Archive the CloudFront log files by moving them into a separate bucket
#
# Note that each step is only actioned if the previous step succeeded without error.
#
# This Ruby script is dependent on:
# 1. AWS::S3 - see https://github.com/marcel/aws-s3/blob/master/INSTALL
# 2. Amazon Elastic MapReduce Ruby Client - see http://aws.amazon.com/developertools/2264
#
# Please make sure that both of these are installed before running this script.
#
# Author::    Alex Dean (mailto:alex@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'config'
require 's3utils'

# TODO: figure out error handling

# First get and load the config
config = Config.get_config()

# Now we upload the Hive query to S3
S3Utils.upload_hive_query()

# Execute the Hive query
# TODO

# TODO: move the below into a separate module
$LOAD_PATH.unshift config[:aws][:emr_client_path]
# TODO: check the EMR client path
require 'commands'
require 'simple_logger'
require 'simple_executor'

exit_code = 0
begin
  logger = SimpleLogger.new
  executor = SimpleExecutor.new
  commands = Commands::create_and_execute_commands(
    ARGV, Amazon::Coral::ElasticMapReduceClient, logger, executor
  )
rescue SystemExit => e
  exit_code = -1
rescue Exception => e
  STDERR.puts("Error: " + e.message)
  STDERR.puts(e.backtrace.join("\n"))
  exit_code = -1
end

exit(exit_code)

# Runs a daily ETL job for the specific day.
# Uses the Elastic MapReduce Command Line Tool.
# Parameters:
# +day+:: the day to run the ETL job for
# +buckets+:: the hash of bucket names to pass in to the Hive script
def run_etl(day, buckets)
  puts day
end

# Finally we move the S3 files into the archive bucket
S3Utils.archive_cloudfront_logs()

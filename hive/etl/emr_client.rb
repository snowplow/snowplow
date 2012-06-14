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

# Author::    Alex Dean (mailto:alex@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'elastic-mapreduce'

# Ruby class to execute jobs against the Amazon Ruby EMR command line (CLI) tool.
# Note that we are wrapping the CLI tool, not the Amazon Ruby EMR client - this
# is because the Ruby client is too low-level: all the functionality around
# building Hive steps etc is found in the CLI tool, not in the Ruby client.
module EmrClient

  # Runs a daily ETL job for the specific day.
  # Uses the Elastic MapReduce Command Line Tool.
  # Parameters:
  # +config+:: the hash of configuration options
  def EmrClient.run_etl(config)
    argv = Array.new(
      "--create",
      "--name", "Daily ETL [%s]" % config[:date],
      "--hive-script", config[:query_file][:remote],
      "--hive-versions", config[:hive_version],
      "--args", "-d,SERDE_FILE=s3://%s" % config[:serde_file],
      "--args", "-d,CLOUDFRONT_LOGS=s3://%s/" % config[:buckets][:in],
      "--args", "-d,EVENTS_TABLE=s3://%s/" % config[:buckets][:out],
      "--args", "-d,DATA_DATE=%s" % config[:date]
    )
    execute(argv)
  end

  # Execute a command using the Amazon EMR client.
  # Syntax taken from Amazon's elastic-map-reduce.rb
  # Parameters:
  # +argv+:: the array of command-line-style arguments to pass to the Amazon EMR client
  def EmrClient.execute(argv)
    logger = SimpleLogger.new
    executor = SimpleExecutor.new
    commands = Commands::create_and_execute_commands(
      argv, Amazon::Coral::ElasticMapReduceClient, logger, executor
    )
  end

end

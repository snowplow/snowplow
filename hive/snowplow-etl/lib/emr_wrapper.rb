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

require 'commands'
require 'simple_logger'
require 'simple_executor'

# Ruby class to execute jobs against the Amazon Ruby EMR command line (CLI) tool.
# Note that we are wrapping the CLI tool, not the Amazon Ruby EMR client - this
# is because the Ruby client is too low-level: all the functionality around
# building Hive steps etc is found in the CLI tool, not in the Ruby client.
module EmrWrapper

  # End states mean we can stop monitoring
  CLOSED_DOWN_STATES = Set.new(%w(TERMINATED COMPLETED FAILED))

  # Executes a command using the Amazon EMR client.
  # Syntax taken from Amazon's elastic-map-reduce.rb
  #
  # Parameters:
  # +argv+:: the array of command-line-style arguments to pass to the
  # Amazon EMR client
  #
  # Returns the jobflow ID
  def execute(argv)

    logger = SimpleLogger.new
    executor = SimpleExecutor.new

    commands = Commands::create_and_execute_commands(
      argv, Amazon::Coral::ElasticMapReduceClient, logger, executor
    )
    commands.global_options[:jobflow]
  end
  module_function :execute

  # Taken from the library's Command class
  def resolve(obj, *args)
    while obj != nil && args.size > 0 do
      obj = obj[args.shift]
    end
    return obj
  end
  module_function :resolve

  # Monitor an EMR job.
  # Check its status every 5 minutes till it completes.
  #
  # Parameters:
  # +config+:: contains our ETL configuration
  # +job_id+:: the ID of the EMR job we want to monitor
  def monitor_job(config, job_id)

    puts "Monitoring job %s" % job_id

    # Configure the client
    client_config = {
      :endpoint            => "https://elasticmapreduce.amazonaws.com",
      :aws_access_key      => config[:aws][:access_key_id],
      :aws_secret_key      => config[:aws][:secret_access_key],
      :signature_algorithm => :V2
    }
    client = Amazon::Coral::ElasticMapReduceClient.new_aws_query(client_config)

    while true do
      jobflow_detail = client.DescribeJobFlows('JobFlowId' => job_id).inspect
      state = resolve(jobflow_detail, "ExecutionStatusDetail", "State")
      puts "Jobflow is in state #{state}, waiting...."

      # sleep 5.minutes
      break
    end

    # TODO: need to handle success or failure
  end
  module_function :monitor_job

  # Runs a daily ETL job for the specific day.
  # Uses the Elastic MapReduce Command Line Tool.
  #
  # Parameters:
  # +config+:: the hash of configuration options
  def EmrWrapper.run_daily_etl(config)

    # Set the environment variables needed for EmrClient
    ENV['ELASTIC_MAPREDUCE_ACCESS_ID'] = config[:aws][:access_key_id]
    ENV['ELASTIC_MAPREDUCE_PRIVATE_KEY'] = config[:aws][:secret_access_key]

    # Now prep the common command-line args
    argv = [
      "--create"
    ]

    # Which date args we supply, and to which script depends on whether
    # we're processing a date range or single day
    script = lambda {|file| return "s3://%s/%s" % [config[:buckets][:query], file]}
    if config[:start] == config[:end] # Processing just one day
      argv.push(
        "--name", "Daily ETL [%s]" % config[:start],
        "--hive-script", script.call(config[:daily_query_file]),
        "--args", "-d,DATA_DATE=%s" % config[:start]
      )
    else # We are processing a datespan
      argv.push(
        "--name", "Daily ETL [%s-%s]" % [config[:start], config[:end]],
        "--hive-script", script.call(config[:datespan_query_file]),
        "--args", "-d,START_DATE=%s" % config[:start],
        "--args", "-d,END_DATE=%s" % config[:end]
      )
    end

    # Add remaining arguments. Because --hive-versions
    # has to follow --hive-script
    argv.push(
      "--hive-versions", config[:hive_version],
      "--args", "-d,SERDE_FILE=s3://%s" % config[:serde_file],
      "--args", "-d,CLOUDFRONT_LOGS=s3://%s/" % config[:buckets][:in],
      "--args", "-d,EVENTS_TABLE=s3://%s/" % config[:buckets][:out]
    )

    puts "Args prepped, now about to execute..."

    jobflow_id = execute(argv)
    monitor_job(config, jobflow_id)
  end

end

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
class EmrClientWrapper
  attr_accessor :access_key_id, :secret_access_key, :monitoring_client

  # End states mean we can stop monitoring
  END_STATES = Set.new(%w(TERMINATED COMPLETED FAILED))

  # Client class is the Coral EMRClient
  EMR_CLASS = Amazon::Coral::ElasticMapReduceClient

  # Initializes our wrapper for the Amazon EMR client.
  #
  # Parameters:
  # +access_key_id+:: the Amazon access ID
  # +secret_access_key+:: the Amazon secret key
  def initialize(access_key_id, secret_access_key)

    @access_key_id = access_key_id
    @secret_access_key = secret_access_key
    @executor = SimpleExecutor.new
    @logger = SimpleLogger.new

    # Set the environment variables needed for EmrClient
    ENV['ELASTIC_MAPREDUCE_ACCESS_ID'] = @access_key_id
    ENV['ELASTIC_MAPREDUCE_PRIVATE_KEY'] = @secret_access_key

    # Configure the monitoring client
    client_config = {
      :endpoint            => "https://elasticmapreduce.amazonaws.com",
      :aws_access_key      => @access_key_id,
      :aws_secret_key      => @secret_access_key,
      :signature_algorithm => :V2
    }
    @monitoring_client = Amazon::Coral::ElasticMapReduceClient.new_aws_query(client_config)
  end

  # Executes a command using the Amazon EMR client.
  # Syntax taken from Amazon's elastic-map-reduce.rb
  #
  # Parameters:
  # +argv+:: the array of command-line-style arguments to pass to the
  # Amazon EMR client
  #
  # Returns the jobflow ID
  def execute(argv)
    commands = Commands::create_and_execute_commands(
      argv, EMR_CLASS, @logger, @executor
    )
    commands.global_options[:jobflow]
  end

  def is_error_response(response)
    response != nil && response.key?('Error')
  end

  def raise_on_error(response)
    if is_error_response(response) then
      raise RuntimeError, response["Error"]["Message"]
    end
    return response
  end

  def resolve(obj, *args)
    while obj != nil && args.size > 0 do
      obj = obj[args.shift]
    end
    return obj
  end

  # Monitor an EMR job.
  # Check its status every 5 minutes till it completes.
  #
  # Parameters:
  # +jobflow_id+:: the ID of the EMR job we want to monitor
  def monitor_job(jobflow_id)

    puts "The jobflow id is #{jobflow_id}"

    while true do
      response = @monitoring_client.DescribeJobFlows('JobFlowId' => jobflow_id)
      raise_on_error(response)
      if response == nil || !response.has_key?("DescribeJobFlowsResult") || response["DescribeJobFlowsResult"]["JobFlows"].size() == 0 then
        raise RuntimeError, "Jobflow with id #{jobflow_id} not found"
      end

      jobflow_detail = response["DescribeJobFlowsResult"]["JobFlows"].first
      jobflow_state = resolve(jobflow_detail, "ExecutionStatusDetail", "State")
      puts "Jobflow is in state #{jobflow_state}, waiting...."

      sleep 10
    end

    # TODO: need to handle success or failure
  end

  # Runs a daily ETL job for the specific day.
  # Uses the Elastic MapReduce Command Line Tool.
  #
  # Parameters:
  # +config+:: the hash of configuration options
  def run_snowplow_etl(config)

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
        "--name", "Datespan ETL [%s-%s]" % [config[:start], config[:end]],
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

    jobflow_id = execute(argv)
    monitor_job(jobflow_id)
  end

end

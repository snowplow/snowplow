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

# Author::    Alex Dean (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'elasticity'

# Ruby class to execute SnowPlow's Hive jobs against Amazon EMR
# using Elasticity (https://github.com/rslifka/elasticity).
class EmrJobs

  # Need to understand the status of all our jobflow steps
  RUNNING_STATES = Set.new(%w(WAITING RUNNING PENDING SHUTTING_DOWN))
  FAILED_STATES  = Set.new(%w(FAILED CANCELLED))

  # Initializes our wrapper for the Amazon EMR client.
  #
  # Parameters:
  # +config+:: contains all the control data for the SnowPlow Hive job
  def initialize(config)

    # Create a job flow with your AWS credentials
    @jobflow = Elasticity::JobFlow.new(config[:aws][:access_key_id], config[:aws][:secret_access_key])

    # Hive configuration if we're processing just one day...
    if config[:start] == config[:end]
      @jobflow.name = "Daily ETL (#{config[:start]})"
      hive_script = config[:daily_query_file]
      hive_args = {
        "DATA_DATE" => config[:start]
      }
    # ...versus processing a datespan
    else
      @jobflow.name = "Datespan ETL (%s-%s)" % [ config[:start], config[:end] ]
      hive_script = config[:datespan_query_file]
      hive_args = {
        "START_DATE" => config[:start],
        "END_DATE"   => config[:end]
      }
    end

    # Additional configuration
    @jobflow.hadoop_version = config[:hadoop_version]
    @jobflow.ec2_key_name = config[:ec2_key_name]
    @jobflow.placement = config[:emr_placement]

    # add extra configuration
    if config[:emr][:jobflow].respond_to?(:each)
      config[:emr][:jobflow].each { |key, value|
        @jobflow.send("#{key}=", value)
      }
    end

    # Now add the Hive step to the jobflow
    hive_step = Elasticity::HiveStep.new("s3n://%s%s" % [config[:s3][:buckets][:assets], hive_script])

    # add extra configuration
    if config[:emr][:hive_step].respond_to?(:each)
      config[:emr][:hive_step].each { |key, value|
        hive_step.send("#{key}=", value)
      }
    end

    hive_step.variables = {
      "SERDE_FILE"      => "s3n://%s%s" % [ config[:s3][:buckets][:serde], config[:serde_file] ],
      "CLOUDFRONT_LOGS" => "s3n://%s" % config[:s3][:buckets][:processing],
      "EVENTS_TABLE"    => "s3n://%s" % config[:s3][:buckets][:out]
    }.merge(hive_args)
    @jobflow.add_step(hive_step)
  end

  # Wait for a jobflow.
  # Check its status every 5 minutes till it completes.
  #
  # Parameters:
  # +jobflow_id+:: the ID of the EMR job we wait for
  #
  # Returns true if the jobflow completed without error,
  # false otherwise.
  def wait_for(jobflow_id)

    # Loop until we can quit...
    while true do
      # Count up running tasks and failures
      statuses = @jobflow.status.steps.map(&:state).inject([0, 0]) do |sum, state|
        [ sum[0] + (RUNNING_STATES.include?(state) ? 1 : 0), sum[1] + (FAILED_STATES.include?(state) ? 1 : 0) ]
      end

      # If no step is still running, then quit
      if statuses[0] == 0
        return statuses[1] == 0 # True if no failures
      end

      # Otherwise sleep a while
      sleep(5) #*60)
    end
  end

  # Run (and wait for) the daily ETL job.
  #
  # Throws a RuntimeError if the jobflow does not succeed.
  def run_etl()

    jobflow_id = @jobflow.run
    status = wait_for(jobflow_id)

    if !status
      raise ExecutionError, "Hive jobflow #{jobflow_id} failed, check Amazon logs for details. Data files not archived."
    end
  end

  class ExecutionError < RuntimeError
  end

end
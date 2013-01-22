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

require 'set'
require 'elasticity'

# Ruby class to execute SnowPlow's Hive jobs against Amazon EMR
# using Elasticity (https://github.com/rslifka/elasticity).
module SnowPlow
  module EmrEtlRunner
    class EmrJob

      # Need to understand the status of all our jobflow steps
      @@running_states = Set.new(%w(WAITING RUNNING PENDING SHUTTING_DOWN))
      @@failed_states  = Set.new(%w(FAILED CANCELLED))

      # Initializes our wrapper for the Amazon EMR client.
      #
      # Parameters:
      # +config+:: contains all the control data for the SnowPlow Hive job
      def initialize(config)

        puts "Initializing EMR jobflow"

        # Create a job flow with your AWS credentials
        @jobflow = Elasticity::JobFlow.new(config[:aws][:access_key_id], config[:aws][:secret_access_key])

        # Set the name of the jobflow
        @jobflow.name = "SnowPlow EmrEtlRunner: %s" % case
                          when (config[:start].nil? and config[:end].nil?)
                            "Rolling mode"
                          when config[:start].nil?
                            "Timespan mode (to %s)" % config[:end]
                          when config[:end].nil?
                            "Timespan mode (%s onwards)" % config[:start]
                          else
                            "Timespan mode (%s to %s)" % [config[:start], config[:end]]
                          end

        # Additional configuration
        @jobflow.hadoop_version = config[:emr][:hadoop_version]
        @jobflow.ec2_key_name = config[:emr][:ec2_key_name]
        @jobflow.placement = config[:emr][:placement]
        @jobflow.log_uri = config[:s3][:buckets][:log]

        # Add extra configuration
        if config[:emr][:jobflow].respond_to?(:each)
          config[:emr][:jobflow].each { |key, value|
            @jobflow.send("#{key}=", value)
          }
        end

        # Now create the Hive step for the jobflow
        hive_step = Elasticity::HiveStep.new(config[:hiveql_asset])

        # Add extra configuration
        if config[:emr][:hive_step].respond_to?(:each)
          config[:emr][:hive_step].each { |key, value|
            hive_step.send("#{key}=", value)
          }
        end

        hive_step.variables = {
          "SERDE_FILE"       => config[:serde_asset],
          "CLOUDFRONT_LOGS"  => config[:s3][:buckets][:processing],
          "EVENTS_TABLE"     => config[:s3][:buckets][:out],
          "COLLECTOR_FORMAT" => config[:etl][:collector_format],
          "CONTINUE_ON"      => config[:etl][:continue_on_unexpected_error]
        }

        # Finally add to our jobflow
        @jobflow.add_step(hive_step)
      end

      # Run (and wait for) the daily ETL job.
      #
      # Throws a RuntimeError if the jobflow does not succeed.
      def run()

        jobflow_id = @jobflow.run
        status = wait_for(jobflow_id)

        if !status
          raise EmrExecutionError, "EMR jobflow #{jobflow_id} failed, check Amazon logs for details. Data files not archived."
        end

        puts "EMR jobflow #{jobflow_id} completed successfully."
      end

      # Wait for a jobflow.
      # Check its status every 2 minutes till it completes.
      #
      # Parameters:
      # +jobflow_id+:: the ID of the EMR job we wait for
      #
      # Returns true if the jobflow completed without error,
      # false otherwise.
      def wait_for(jobflow_id)

        # Loop until we can quit...
        while true do
          begin 
            # Count up running tasks and failures
            statuses = @jobflow.status.steps.map(&:state).inject([0, 0]) do |sum, state|
              [ sum[0] + (@@running_states.include?(state) ? 1 : 0), sum[1] + (@@failed_states.include?(state) ? 1 : 0) ]
            end

            # If no step is still running, then quit
            if statuses[0] == 0
              return statuses[1] == 0 # True if no failures
            end

            # Sleep a while before we check again
            sleep(120)

          rescue SocketError => se
            puts "Got socket error #{se}, waiting 2 minutes before checking jobflow again"
            sleep(300)
          end
        end
      end
    end

  end
end
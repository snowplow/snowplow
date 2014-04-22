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

        # Configure
        @jobflow.name = config[:etl][:job_name]
        @jobflow.hadoop_version = config[:emr][:hadoop_version]
        @jobflow.ec2_key_name = config[:emr][:ec2_key_name]
        @jobflow.placement = config[:emr][:placement]
        @jobflow.ec2_subnet_id = config[:emr][:ec2_subnet_id]
        @jobflow.log_uri = config[:s3][:buckets][:log]
        @jobflow.enable_debugging = config[:debug]
        @jobflow.visible_to_all_users = true

        # Add bootstap action
        # Ideal buffer size for S3 and look in user classpath first
        if config[:etl][:collector_format] == 'thrift-raw'
            [
              Elasticity::HadoopBootstrapAction.new('-s', 'io.file.buffer.size=65536'),
              Elasticity::HadoopBootstrapAction.new('-m', 'mapreduce.user.classpath.first=true')
            ].each do |action|
              @jobflow.add_bootstrap_action(action)
            end
        end

        # Add extra configuration
        if config[:emr][:jobflow].respond_to?(:each)
          config[:emr][:jobflow].each { |key, value|
            k = key.to_s
            # Don't include the task fields
            if not k.start_with?("task_")
              @jobflow.send("#{k}=", value)
            end
          }
        end

        # Now let's add our task group if required
        tic = config[:emr][:jobflow][:task_instance_count]
        unless tic.nil? or tic == 0
          ig = Elasticity::InstanceGroup.new
          ig.count = tic
          ig.type  = config[:emr][:jobflow][:task_instance_type]
          tib = config[:emr][:jobflow][:task_instance_bid]
          if tib.nil?
            ig.set_on_demand_instances
          else
            ig.set_spot_instances(tib)
          end
          @jobflow.set_task_instance_group(ig)
        end

        # We only consolidate files on HDFS folder for CloudFront currently
        unless config[:etl][:collector_format] == "cloudfront"
          hadoop_input = config[:s3][:buckets][:processing]

        else
          hadoop_input = "hdfs:///local/snowplow-logs"

          # Create the Hadoop MR step for the file crushing
          filecrush_step = Elasticity::CustomJarStep.new(config[:s3distcp_asset])

          filecrush_step.arguments = [
            "--src"               , config[:s3][:buckets][:processing],
            "--dest"              , hadoop_input,
            "--groupBy"           , ".*\\.([0-9]+-[0-9]+-[0-9]+)-[0-9]+\\..*",
            "--targetSize"        , "128",
            "--outputCodec"       , "lzo",
            "--s3Endpoint"        , config[:s3][:endpoint],
          ]

          # Add to our jobflow
          @jobflow.add_step(filecrush_step)
        end

        # Now create the Hadoop MR step for the jobflow
        hadoop_step = Elasticity::CustomJarStep.new(config[:hadoop_asset])

        # Add extra configuration (undocumented feature)
        if config[:emr][:hadoop_step].respond_to?(:each)
          config[:emr][:hadoop_step].each { |key, value|
            hadoop_step.send("#{key}=", value)
          }
        end

        # We need to partition our output buckets by run ID
        # Note buckets already have trailing slashes
        partition = lambda { |bucket| "#{bucket}run%3D#{config[:run_id]}/" } # TODO: s/%3D/=/ when Scalding Args supports it

        hadoop_step.arguments = [
          "com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob", # Job to run
          "--hdfs", # Always --hdfs mode, never --local
          "--input_folder"      , hadoop_input, # Argument names are "--arguments" too
          "--input_format"      , config[:etl][:collector_format],
          "--maxmind_file"      , config[:maxmind_asset],
          "--output_folder"     , partition.call(config[:s3][:buckets][:out]),
          "--bad_rows_folder"   , partition.call(config[:s3][:buckets][:out_bad_rows]),
          "--anon_ip_quartets"  , config[:enrichments][:anon_ip_octets]
        ]

        # Conditionally add exceptions_folder
        if config[:etl][:continue_on_unexpected_error]
          hadoop_step.arguments.concat [
            "--exceptions_folder" , partition.call(config[:s3][:buckets][:out_errors])
          ]
        end

        # Finally add to our jobflow
        @jobflow.add_step(hadoop_step)
      end

      # Run (and wait for) the daily ETL job.
      #
      # Throws a RuntimeError if the jobflow does not succeed.
      def run()

        jobflow_id = @jobflow.run
        puts "EMR jobflow #{jobflow_id} started, waiting for jobflow to complete..."
        status = wait_for(jobflow_id)

        if !status
          raise EmrExecutionError, "EMR jobflow #{jobflow_id} failed, check Amazon EMR console and Hadoop logs for details (help: https://github.com/snowplow/snowplow/wiki/Troubleshooting-jobs-on-Elastic-MapReduce). Data files not archived."
        end

        puts "EMR jobflow #{jobflow_id} completed successfully."
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
            puts "Got socket error #{se}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          end
        end
      end
    end

  end
end

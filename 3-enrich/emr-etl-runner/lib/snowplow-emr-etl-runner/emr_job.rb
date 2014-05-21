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

# Author::    Alex Dean (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'set'
require 'elasticity'

require 'contracts'
include Contracts

# Ruby class to execute Snowplow's Hive jobs against Amazon EMR
# using Elasticity (https://github.com/rslifka/elasticity).
module Snowplow
  module EmrEtlRunner
    class EmrJob

      # Need to understand the status of all our jobflow steps
      @@running_states = Set.new(%w(WAITING RUNNING PENDING SHUTTING_DOWN))
      @@failed_states  = Set.new(%w(FAILED CANCELLED))

      include Logging

      # Initializes our wrapper for the Amazon EMR client.
      Contract Bool, ConfigHash => EmrJob
      def initialize(debug, config)

        logger.debug "Initializing EMR jobflow"

        # Configuration
        assets = self.class.get_assets(config[:s3][:buckets][:assets], config[:etl][:hadoop_etl_version])
        run_id = Time.new.strftime("%Y-%m-%d-%H-%M-%S")

        # Create a job flow with your AWS credentials
        @jobflow = Elasticity::JobFlow.new(config[:aws][:access_key_id], config[:aws][:secret_access_key])

        # Configure
        @jobflow.name                 = config[:etl][:job_name]
        @jobflow.ami_version          = config[:emr][:ami_version]
        @jobflow.ec2_key_name         = config[:emr][:ec2_key_name]

        @jobflow.instance_variable_set(:@region, config[:emr][:region]) # Workaround until https://github.com/snowplow/snowplow/issues/753
        @jobflow.placement            = config[:emr][:placement]
        unless @jobflow.ec2_subnet_id.nil? # Nils placement so do last
          @jobflow.ec2_subnet_id      = config[:emr][:ec2_subnet_id]
        end

        @jobflow.log_uri              = config[:s3][:buckets][:log]
        @jobflow.enable_debugging     = debug
        @jobflow.visible_to_all_users = true

        @jobflow.instance_count       = config[:emr][:jobflow][:core_instance_count] + 1 # +1 for the master instance
        @jobflow.master_instance_type = config[:emr][:jobflow][:master_instance_type]
        @jobflow.slave_instance_type  = config[:emr][:jobflow][:core_instance_type]

        # Now let's add our task group if required
        tic = config[:emr][:jobflow][:task_instance_count]
        if tic > 0
          instance_group = Elasticity::InstanceGroup.new.tap { |ig|
            ig.count = tic
            ig.type  = config[:emr][:jobflow][:task_instance_type]
            
            tib = config[:emr][:jobflow][:task_instance_bid]
            if tib.nil?
              ig.set_on_demand_instances
            else
              ig.set_spot_instances(tib)
            end
          }

          @jobflow.set_task_instance_group(instance_group)
        end

        # We only consolidate files on HDFS folder for CloudFront currently
        unless config[:etl][:collector_format] == "cloudfront"
          hadoop_input = config[:s3][:buckets][:processing]
        
        else
          hadoop_input = "hdfs:///local/snowplow-logs"

          # Create the Hadoop MR step for the file crushing
          filecrush_step = Elasticity::CustomJarStep.new(assets[:s3distcp])

          filecrush_step.arguments = [
            "--src"               , config[:s3][:buckets][:processing],
            "--dest"              , hadoop_input,
            "--groupBy"           , ".*\\.([0-9]+-[0-9]+-[0-9]+)-[0-9]+\\..*",
            "--targetSize"        , "128",
            "--outputCodec"       , "lzo",
            "--s3Endpoint"        , self.class.get_s3_endpoint(config[:s3][:region]),
          ]

          # Add to our jobflow
          @jobflow.add_step(filecrush_step)
        end

        # Now create the Hadoop MR step for the jobflow
        hadoop_step = Elasticity::CustomJarStep.new(assets[:hadoop])

        # Add extra configuration (undocumented feature)
        if config[:emr][:hadoop_step].respond_to?(:each)
          config[:emr][:hadoop_step].each { |key, value|
            hadoop_step.send("#{key}=", value)
          }
        end

        # We need to partition our output buckets by run ID
        # Note buckets already have trailing slashes
        partition = lambda { |bucket| "#{bucket}run%3D#{run_id}/" } # TODO: s/%3D/=/ when Scalding Args supports it

        hadoop_step.arguments = [
          "com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob", # Job to run
          "--hdfs", # Always --hdfs mode, never --local
          "--input_folder"      , hadoop_input, # Argument names are "--arguments" too
          "--input_format"      , config[:etl][:collector_format],
          "--maxmind_file"      , assets[:maxmind],
          "--output_folder"     , partition.call(config[:s3][:buckets][:out]),
          "--bad_rows_folder"   , partition.call(config[:s3][:buckets][:out_bad_rows]),
          "--anon_ip_quartets"  , self.class.get_anon_ip_octets(config[:enrichments][:anon_ip])
        ]

        # Conditionally add exceptions_folder
        if config[:etl][:continue_on_unexpected_error]
          hadoop_step.arguments.concat [
            "--exceptions_folder" , partition.call(config[:s3][:buckets][:out_errors])
          ]
        end

        # Finally add to our jobflow
        @jobflow.add_step(hadoop_step)

        self
      end

      # Run (and wait for) the daily ETL job.
      #
      # Throws a RuntimeError if the jobflow does not succeed.
      Contract None => nil
      def run()

        jobflow_id = @jobflow.run
        logger.debug "EMR jobflow #{jobflow_id} started, waiting for jobflow to complete..."
        status = wait_for()

        if !status
          raise EmrExecutionError, "EMR jobflow #{jobflow_id} failed, check Amazon EMR console and Hadoop logs for details (help: https://github.com/snowplow/snowplow/wiki/Troubleshooting-jobs-on-Elastic-MapReduce). Data files not archived."
        end

        logger.debug "EMR jobflow #{jobflow_id} completed successfully."
        nil
      end

    private

      # Wait for a jobflow.
      # Check its status every 5 minutes till it completes.
      #
      # Returns true if the jobflow completed without error,
      # false otherwise.
      Contract None => Bool
      def wait_for()

        success = false

        # Loop until we can quit...
        while true do
          begin
            # Count up running tasks and failures
            statuses = @jobflow.status.steps.map(&:state).inject([0, 0]) do |sum, state|
              [ sum[0] + (@@running_states.include?(state) ? 1 : 0), sum[1] + (@@failed_states.include?(state) ? 1 : 0) ]
            end

            # If no step is still running, then quit
            if statuses[0] == 0
              success = statuses[1] == 0 # True if no failures
              break
            else
              # Sleep a while before we check again
              sleep(120)              
            end

          rescue SocketError => se
            logger.warn "Got socket error #{se}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          end
        end

        success
      end

      Contract AnonIpHash => String
      def self.get_anon_ip_octets(anon_ip)
        if anon_ip[:enabled]
          anon_ip[:anon_octets].to_s
        else
          '0' # Anonymize 0 quartets == anonymization disabled
        end
      end

      Contract String, String => AssetsHash
      def self.get_assets(assets_bucket, hadoop_etl_version)

        asset_host = 
          if assets_bucket == "s3://snowplow-hosted-assets/"
            "http://snowplow-hosted-assets.s3.amazonaws.com/" # Use the public S3 URL
          else
            assets_bucket
          end

        { :maxmind  => "#{asset_host}third-party/maxmind/GeoLiteCity.dat",
          :s3distcp => "/home/hadoop/lib/emr-s3distcp-1.0.jar",
          :hadoop   => "#{assets_bucket}3-enrich/hadoop-etl/snowplow-hadoop-etl-#{hadoop_etl_version}.jar"
        }
      end

      # Returns the S3 endpoint for a given
      # S3 region
      Contract String => String
      def self.get_s3_endpoint(s3_region)
        if s3_region == "us-east-1"
          "s3.amazonaws.com"
        else
          "s3-#{s3_region}.amazonaws.com"
        end
      end

    end
  end
end

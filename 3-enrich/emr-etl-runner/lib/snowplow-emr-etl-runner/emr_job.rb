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
require 'sluice'
require 'awrence'
require 'json'
require 'base64'
require 'contracts'

# Ruby class to execute Snowplow's Hive jobs against Amazon EMR
# using Elasticity (https://github.com/rslifka/elasticity).
module Snowplow
  module EmrEtlRunner
    class EmrJob

      include Contracts

      # Constants
      JAVA_PACKAGE = "com.snowplowanalytics.snowplow"
      PARTFILE_REGEXP = ".*part-.*"

      # Need to understand the status of all our jobflow steps
      @@running_states = Set.new(%w(WAITING RUNNING PENDING SHUTTING_DOWN))
      @@failed_states  = Set.new(%w(FAILED CANCELLED))

      include Logging

      # Initializes our wrapper for the Amazon EMR client.
      Contract Bool, Bool, Bool, Bool, ConfigHash, ArrayOf[String] => EmrJob
      def initialize(debug, enrich, shred, s3distcp, config, enrichments_array)

        logger.debug "Initializing EMR jobflow"

        # Configuration
        assets = self.class.get_assets(config[:s3][:buckets][:assets], config[:etl][:versions][:hadoop_enrich], config[:etl][:versions][:hadoop_shred])
        run_tstamp = Time.new
        run_id = run_tstamp.strftime("%Y-%m-%d-%H-%M-%S")
        etl_tstamp = (run_tstamp.to_f * 1000).to_i.to_s
        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Create a job flow with your AWS credentials
        @jobflow = Elasticity::JobFlow.new(config[:aws][:access_key_id], config[:aws][:secret_access_key])

        # Configure
        @jobflow.name                 = config[:etl][:job_name]
        @jobflow.ami_version          = config[:emr][:ami_version]
        @jobflow.ec2_key_name         = config[:emr][:ec2_key_name]

        @jobflow.region               = config[:emr][:region]
        @jobflow.job_flow_role        = config[:emr][:jobflow_role]
        @jobflow.placement            = config[:emr][:placement]
        unless config[:emr][:ec2_subnet_id].nil? # Nils placement so do last and conditionally
          @jobflow.ec2_subnet_id      = config[:emr][:ec2_subnet_id]
        end

        @jobflow.log_uri              = config[:s3][:buckets][:log]
        @jobflow.enable_debugging     = debug
        @jobflow.visible_to_all_users = true

        @jobflow.instance_count       = config[:emr][:jobflow][:core_instance_count] + 1 # +1 for the master instance
        @jobflow.master_instance_type = config[:emr][:jobflow][:master_instance_type]
        @jobflow.slave_instance_type  = config[:emr][:jobflow][:core_instance_type]

        if config[:etl][:collector_format] == 'thrift'
          [
            Elasticity::HadoopBootstrapAction.new('-s', 'io.file.buffer.size=65536'),
            Elasticity::HadoopBootstrapAction.new('-m', 'mapreduce.user.classpath.first=true')
          ].each do |action|
            @jobflow.add_bootstrap_action(action)
          end
        end

        # Add custom bootstrap actions
        bootstrap_actions = config[:emr][:bootstrap]
        bootstrap_actions.each do |bootstrap_action|
          @jobflow.add_bootstrap_action(Elasticity::BootstrapAction.new(bootstrap_action))
        end

        # Install and launch HBase
        hbase = config[:emr][:software][:hbase]
        unless not hbase
          install_hbase_action = Elasticity::BootstrapAction.new("s3://#{config[:emr][:region]}.elasticmapreduce/bootstrap-actions/setup-hbase")
          @jobflow.add_bootstrap_action(install_hbase_action)

          start_hbase_step = Elasticity::CustomJarStep.new("/home/hadoop/lib/hbase-#{hbase}.jar")
          start_hbase_step.name = "Start HBase #{hbase}"
          start_hbase_step.arguments = [ 'emr.hbase.backup.Main', '--start-master' ]
          @jobflow.add_step(start_hbase_step)
        end

        # Install Lingual
        lingual = config[:emr][:software][:lingual]
        unless not lingual
          install_lingual_action = Elasticity::BootstrapAction.new("s3://files.concurrentinc.com/lingual/#{lingual}/lingual-client/install-lingual-client.sh")
          @jobflow.add_bootstrap_action(install_lingual_action)
        end

        # For serialization debugging. TODO doesn't work yet
        # install_ser_debug_action = Elasticity::BootstrapAction.new("s3://snowplow-hosted-assets/common/emr/cascading-ser-debug.sh")
        # @jobflow.add_bootstrap_action(install_ser_debug_action)

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

        s3_endpoint = self.class.get_s3_endpoint(config[:s3][:region])
        csbr = config[:s3][:buckets][:raw]
        csbe = config[:s3][:buckets][:enriched]

        enrich_final_output = if enrich
          self.class.partition_by_run(csbe[:good], run_id)
        else
          csbe[:good] # Doesn't make sense to partition if enrich has already been done
        end
        enrich_step_output = if s3distcp
          "hdfs:///local/snowplow/enriched-events/"
        else
          enrich_final_output
        end

        if enrich

          # 1. Compaction to HDFS (only for CloudFront currently)
          raw_input = csbr[:processing]
          to_hdfs = (self.class.is_cloudfront_log(config[:etl][:collector_format]) and s3distcp)

          enrich_step_input = if to_hdfs
            "hdfs:///local/snowplow/raw-events/"
          else
            raw_input
          end

          if to_hdfs
            # Create the Hadoop MR step for the file crushing
            compact_to_hdfs_step = Elasticity::S3DistCpStep.new
            compact_to_hdfs_step.arguments = [
              "--src"         , raw_input,
              "--dest"        , enrich_step_input,
              "--groupBy"     , ".*\\.([0-9]+-[0-9]+-[0-9]+)-[0-9]+\\..*",
              "--targetSize"  , "128",
              "--outputCodec" , "lzo",
              "--s3Endpoint"  , s3_endpoint
            ]
            compact_to_hdfs_step.name << ": Raw S3 -> HDFS"

            # Add to our jobflow
            @jobflow.add_step(compact_to_hdfs_step)
          end

          # 2. Enrichment
          enrich_step_output = if s3distcp
            "hdfs:///local/snowplow/enriched-events/"
          else
            enrich_final_output
          end

          enrich_step = build_scalding_step(
            "Enrich Raw Events",
            assets[:enrich],
            "enrich.hadoop.EtlJob",
            { :in     => enrich_step_input,
              :good   => enrich_step_output,
              :bad    => self.class.partition_by_run(csbe[:bad],    run_id),
              :errors => self.class.partition_by_run(csbe[:errors], run_id, config[:etl][:continue_on_unexpected_error])
            },
            { :input_format     => config[:etl][:collector_format],
              :etl_tstamp       => etl_tstamp,
              :iglu_config      => self.class.build_iglu_config_json(config[:iglu]),
              :enrichments      => self.class.build_enrichments_json(enrichments_array)
            }
          )

          # Late check whether our enrichment directory is empty. We do an early check too
          csbe_good_loc = Sluice::Storage::S3::Location.new(csbe[:good])
          unless Sluice::Storage::S3::is_empty?(s3, csbe_good_loc)
            raise DirectoryNotEmptyError, "Cannot safely add enrichment step to jobflow, #{csbe_good_loc} is not empty"
          end
          @jobflow.add_step(enrich_step)

          if s3distcp
            # We need to copy our enriched events from HDFS back to S3
            copy_to_s3_step = Elasticity::S3DistCpStep.new
            copy_to_s3_step.arguments = [
              "--src"        , enrich_step_output,
              "--dest"       , enrich_final_output,
              "--srcPattern" , PARTFILE_REGEXP,
              "--s3Endpoint" , s3_endpoint
            ]
            copy_to_s3_step.name << ": Enriched HDFS -> S3"
            @jobflow.add_step(copy_to_s3_step)
          end
        end

        if shred

          # 3. Shredding
          csbs = config[:s3][:buckets][:shredded]
          shred_final_output = self.class.partition_by_run(csbs[:good], run_id)
          shred_step_output = if s3distcp
            "hdfs:///local/snowplow/shredded-events/"
          else
            shred_final_output
          end

          # If we didn't enrich already, we need to copy to HDFS
          if s3distcp and !enrich
            copy_to_hdfs_step = Elasticity::S3DistCpStep.new
            copy_to_hdfs_step.arguments = [
              "--src"        , enrich_final_output, # Opposite way round to normal
              "--dest"       , enrich_step_output,
              "--srcPattern" , PARTFILE_REGEXP,
              "--s3Endpoint" , s3_endpoint
            ]
            copy_to_hdfs_step.name << ": Enriched S3 -> HDFS"
            @jobflow.add_step(copy_to_hdfs_step)
          end

          shred_step = build_scalding_step(
            "Shred Enriched Events",
            assets[:shred],
            "enrich.hadoop.ShredJob",
            { :in          => enrich_step_output,
              :good        => shred_step_output,
              :bad         => self.class.partition_by_run(csbs[:bad],    run_id),
              :errors      => self.class.partition_by_run(csbs[:errors], run_id, config[:etl][:continue_on_unexpected_error])
            },
            {
              :iglu_config => self.class.build_iglu_config_json(config[:iglu])
            }
          )

          # Late check whether our target directory is empty
          csbs_good_loc = Sluice::Storage::S3::Location.new(csbs[:good])
          unless Sluice::Storage::S3::is_empty?(s3, csbs_good_loc)
            raise DirectoryNotEmptyError, "Cannot safely add shredding step to jobflow, #{csbs_good_loc} is not empty"
          end
          @jobflow.add_step(shred_step)

          if s3distcp
            # We need to copy our shredded types from HDFS back to S3
            copy_to_s3_step = Elasticity::S3DistCpStep.new
            copy_to_s3_step.arguments = [
              "--src"        , shred_step_output,
              "--dest"       , shred_final_output,
              "--srcPattern" , PARTFILE_REGEXP,
              "--s3Endpoint" , s3_endpoint
            ]
            copy_to_s3_step.name << ": Shredded HDFS -> S3"
            @jobflow.add_step(copy_to_s3_step)
          end
        end

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
          raise EmrExecutionError, get_failure_details()
        end

        logger.debug "EMR jobflow #{jobflow_id} completed successfully."
        nil
      end

    private

      # Defines a Scalding data processing job as an Elasticity
      # jobflow step.
      #
      # Parameters:
      # +step_name+:: name of step
      # +main_class+:: Java main class to run
      # +folders+:: hash of in, good, bad, errors S3/HDFS folders
      # +extra_step_args+:: additional arguments to pass to the step
      #
      # Returns a step ready for adding to the jobflow.
      Contract String, String, String, Hash, Hash => ScaldingStep
      def build_scalding_step(step_name, jar, main_class, folders, extra_step_args={})

        # Build our argument hash
        arguments = extra_step_args
          .merge({
            :input_folder      => folders[:in],
            :output_folder     => folders[:good],
            :bad_rows_folder   => folders[:bad],
            :exceptions_folder => folders[:errors]
          })
          .reject { |k, v| v.nil? } # Because folders[:errors] may be empty

        # Now create the Hadoop MR step for the jobflow
        scalding_step = ScaldingStep.new(jar, "#{JAVA_PACKAGE}.#{main_class}", arguments)
        scalding_step.name << ": #{step_name}"

        scalding_step
      end

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
          rescue Errno::ECONNREFUSED => ref
            logger.warn "Got connection refused #{ref}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue Errno::ECONNRESET => res
            logger.warn "Got connection reset #{res}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          end
        end

        success
      end

      # Prettified string containing failure details
      # for this job flow.
      Contract None => String
      def get_failure_details()

        js = @jobflow.status

        [
          "EMR jobflow #{js.jobflow_id} failed, check Amazon EMR console and Hadoop logs for details (help: https://github.com/snowplow/snowplow/wiki/Troubleshooting-jobs-on-Elastic-MapReduce). Data files not archived.",
          "#{js.name}: #{js.state} [#{js.last_state_change_reason}] ~ #{self.class.get_elapsed_time(js.started_at, js.ended_at)} #{self.class.get_timespan(js.started_at, js.ended_at)}"
        ].concat(js.steps
            .sort { |a,b|
              self.class.nilable_spaceship(a.started_at, b.started_at)
            }
            .each_with_index
            .map { |s,i|
              " - #{i + 1}. #{s.name}: #{s.state} ~ #{self.class.get_elapsed_time(s.started_at, s.ended_at)} #{self.class.get_timespan(s.started_at, s.ended_at)}"
            })
          .join("\n")
      end

      # Gets the time span.
      #
      # Parameters:
      # +start+:: start time
      # +_end+:: end time
      Contract Maybe[Time], Maybe[Time] => String
      def self.get_timespan(start, _end)
        "[#{start} - #{_end}]"
      end

      # Spaceship operator supporting nils
      #
      # Parameters:
      # +a+:: First argument
      # +b+:: Second argument
      Contract Maybe[Time], Maybe[Time] => Num
      def self.nilable_spaceship(a, b)
        case
        when (a.nil? and b.nil?)
          0
        when a.nil?
          1
        when b.nil?
          -1
        else
          a <=> b
        end
      end

      # Gets the elapsed time in a
      # human-readable format.
      #
      # Parameters:
      # +start+:: start time
      # +_end+:: end time
      Contract Maybe[Time], Maybe[Time] => String
      def self.get_elapsed_time(start, _end)
        if start.nil? or _end.nil?
          "elapsed time n/a"
        else
          # Adapted from http://stackoverflow.com/a/19596579/255627
          seconds_diff = (start - _end).to_i.abs

          hours = seconds_diff / 3600
          seconds_diff -= hours * 3600

          minutes = seconds_diff / 60
          seconds_diff -= minutes * 60

          seconds = seconds_diff

          "#{hours.to_s.rjust(2, '0')}:#{minutes.to_s.rjust(2, '0')}:#{seconds.to_s.rjust(2, '0')}"
        end
      end

      # Does this collector format represent CloudFront
      # access logs?
      Contract String => Bool
      def self.is_cloudfront_log(collector_format)
        (collector_format == "cloudfront" or collector_format.start_with?("tsv/com.amazon.aws.cloudfront/"))
      end

      # We need to partition our output buckets by run ID
      # Note buckets already have trailing slashes
      #
      # Parameters:
      # +folder+:: the folder to append a run ID folder to
      # +run_id+:: the run ID to append
      # +retain+:: set to false if this folder should be nillified
      #
      # Return the folder with a run ID folder appended
      Contract Maybe[String], String, Bool => Maybe[String]
      def self.partition_by_run(folder, run_id, retain=true)
        "#{folder}run=#{run_id}/" if retain
      end

      # Returns a base64-encoded JSON containing an array of enrichment JSONs
      Contract ArrayOf[String] => String
      def self.build_enrichments_json(enrichments_array)
        enrichments_json_data = enrichments_array.map {|e| JSON.parse(e)}
        enrichments_json = {
          'schema' => 'iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0',
          'data'   => enrichments_json_data
        }

        Base64.strict_encode64(enrichments_json.to_json)
      end

      Contract IgluConfigHash => String
      def self.build_iglu_config_json(iglu_hash)
        Base64.strict_encode64(iglu_hash.to_camelback_keys.to_json)
      end

      Contract String, String, String => AssetsHash
      def self.get_assets(assets_bucket, hadoop_enrich_version, hadoop_shred_version)
        {
          :enrich   => "#{assets_bucket}3-enrich/hadoop-etl/snowplow-hadoop-etl-#{hadoop_enrich_version}.jar",
          :shred    => "#{assets_bucket}3-enrich/scala-hadoop-shred/snowplow-hadoop-shred-#{hadoop_shred_version}.jar",
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

# Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
require 'iglu-client'
require 'securerandom'
require 'tempfile'
require 'fog'

# Ruby class to execute Snowplow's Hive jobs against Amazon EMR
# using Elasticity (https://github.com/rslifka/elasticity).
module Snowplow
  module EmrEtlRunner
    class EmrJob

      include Contracts

      # Constants
      JAVA_PACKAGE = "com.snowplowanalytics.snowplow"
      PARTFILE_REGEXP = ".*part-.*"
      SUCCESS_REGEXP = ".*_SUCCESS"
      STANDARD_HOSTED_ASSETS = "s3://snowplow-hosted-assets"
      ENRICH_STEP_INPUT = 'hdfs:///local/snowplow/raw-events/'
      ENRICH_STEP_OUTPUT = 'hdfs:///local/snowplow/enriched-events/'
      SHRED_STEP_OUTPUT = 'hdfs:///local/snowplow/shredded-events/'

      # Need to understand the status of all our jobflow steps
      @@running_states = Set.new(%w(WAITING RUNNING PENDING SHUTTING_DOWN))
      @@failed_states  = Set.new(%w(FAILED CANCELLED))

      include Monitoring::Logging
      include Snowplow::EmrEtlRunner::Utils

      # Initializes our wrapper for the Amazon EMR client.
      Contract Bool, Bool, Bool, Bool, Bool, Bool, Bool, ArchiveStep, ArchiveStep, ConfigHash, ArrayOf[String], String, TargetsHash, RdbLoaderSteps => EmrJob
      def initialize(debug, staging, enrich, shred, es, archive_raw, rdb_load, archive_enriched, archive_shredded, config, enrichments_array, resolver, targets, rdbloader_steps)

        logger.debug "Initializing EMR jobflow"

        # Configuration
        custom_assets_bucket =
          get_hosted_assets_bucket(STANDARD_HOSTED_ASSETS, config[:aws][:s3][:buckets][:assets], config[:aws][:emr][:region])
        standard_assets_bucket =
          get_hosted_assets_bucket(STANDARD_HOSTED_ASSETS, STANDARD_HOSTED_ASSETS, config[:aws][:emr][:region])
        assets = get_assets(
          custom_assets_bucket,
          config[:enrich][:versions][:spark_enrich],
          config[:storage][:versions][:rdb_shredder],
          config[:storage][:versions][:hadoop_elasticsearch],
          config[:storage][:versions][:rdb_loader])

        collector_format = config[:collectors][:format]
        run_tstamp = Time.new
        run_id = run_tstamp.strftime("%Y-%m-%d-%H-%M-%S")
        @run_id = run_id
        @rdb_loader_log_base = config[:aws][:s3][:buckets][:log] + "rdb-loader/#{@run_id}/"
        @rdb_loader_logs = []   # pairs of target name and associated log
        etl_tstamp = (run_tstamp.to_f * 1000).to_i.to_s
        output_codec = output_codec_from_compression_format(config[:enrich][:output_compression])

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:aws][:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Configure Elasticity with your AWS credentials
        Elasticity.configure do |c|
          c.access_key = config[:aws][:access_key_id]
          c.secret_key = config[:aws][:secret_access_key]
        end

        # Create a job flow
        @jobflow = Elasticity::JobFlow.new

        # Configure
        @jobflow.name                 = config[:aws][:emr][:jobflow][:job_name]

        if config[:aws][:emr][:ami_version] =~ /^[1-3].*/
          @legacy = true
          @jobflow.ami_version = config[:aws][:emr][:ami_version]
        else
          @legacy = false
          @jobflow.release_label = "emr-#{config[:aws][:emr][:ami_version]}"
        end

        @jobflow.tags                 = config[:monitoring][:tags]
        @jobflow.ec2_key_name         = config[:aws][:emr][:ec2_key_name]

        @jobflow.region               = config[:aws][:emr][:region]
        @jobflow.job_flow_role        = config[:aws][:emr][:jobflow_role] # Note job_flow vs jobflow
        @jobflow.service_role         = config[:aws][:emr][:service_role]
        @jobflow.placement            = config[:aws][:emr][:placement]
        @jobflow.additional_info      = config[:aws][:emr][:additional_info]
        unless config[:aws][:emr][:ec2_subnet_id].nil? # Nils placement so do last and conditionally
          @jobflow.ec2_subnet_id      = config[:aws][:emr][:ec2_subnet_id]
        end

        @jobflow.log_uri              = config[:aws][:s3][:buckets][:log]
        @jobflow.enable_debugging     = debug
        @jobflow.visible_to_all_users = true

        @jobflow.instance_count       = config[:aws][:emr][:jobflow][:core_instance_count] + 1 # +1 for the master instance
        @jobflow.master_instance_type = config[:aws][:emr][:jobflow][:master_instance_type]
        @jobflow.slave_instance_type  = config[:aws][:emr][:jobflow][:core_instance_type]

        s3_endpoint = get_s3_endpoint(config[:aws][:s3][:region])
        csbr = config[:aws][:s3][:buckets][:raw]
        csbe = config[:aws][:s3][:buckets][:enriched]
        csbs = config[:aws][:s3][:buckets][:shredded]

        # staging
        if staging
          csbr_processing_loc = Sluice::Storage::S3::Location.new(csbr[:processing])
          unless Sluice::Storage::S3::is_empty?(s3, csbr_processing_loc)
            raise DirectoryNotEmptyError, "Cannot safely add staging step to jobflow, #{csbr_processing_loc} is not empty"
          end

          src_pattern = collector_format == 'clj-tomcat' ? '.*localhost\_access\_log.*\.txt.*' : '.+'
          src_pattern_regex = Regexp.new src_pattern
          non_empty_locs = csbr[:in].select { |l|
            loc = Sluice::Storage::S3::Location.new(l)
            files = Sluice::Storage::S3::list_files(s3, loc)
              .select { |f| !(f.key =~ src_pattern_regex).nil? }
            files.length > 0
          }

          if non_empty_locs.empty?
            raise NoDataToProcessError, "No Snowplow logs to process since last run"
          else
            non_empty_locs.each { |l|
              staging_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
              staging_step.arguments = [
                "--src", l,
                "--dest", csbr[:processing],
                "--s3Endpoint", s3_endpoint,
                "--srcPattern", src_pattern,
                "--deleteOnSuccess"
              ]
              if collector_format == 'clj-tomcat'
                staging_step.arguments = staging_step.arguments + [ '--groupBy', '.*/_*(.+)' ]
              end
              staging_step.name << ": Raw #{l} -> Raw Staging S3"
              @jobflow.add_step(staging_step)
            }
          end
        end

        # EBS
        unless config[:aws][:emr][:jobflow][:core_instance_ebs].nil?
          ebs_bdc = Elasticity::EbsBlockDeviceConfig.new

          ebs_bdc.volume_type          = config[:aws][:emr][:jobflow][:core_instance_ebs][:volume_type]
          ebs_bdc.size_in_gb           = config[:aws][:emr][:jobflow][:core_instance_ebs][:volume_size]
          ebs_bdc.volumes_per_instance = 1
          if config[:aws][:emr][:jobflow][:core_instance_ebs][:volume_type] == "io1"
            ebs_bdc.iops = config[:aws][:emr][:jobflow][:core_instance_ebs][:volume_iops]
          end

          ebs_c = Elasticity::EbsConfiguration.new
          ebs_c.add_ebs_block_device_config(ebs_bdc)
          ebs_c.ebs_optimized = true

          unless config[:aws][:emr][:jobflow][:core_instance_ebs][:ebs_optimized].nil?
            ebs_c.ebs_optimized = config[:aws][:emr][:jobflow][:core_instance_ebs][:ebs_optimized]
          end

          @jobflow.set_core_ebs_configuration(ebs_c)
        end
        @jobflow.add_application("Hadoop")

        if collector_format == 'thrift'
          if @legacy
            [
              Elasticity::HadoopBootstrapAction.new('-c', 'io.file.buffer.size=65536'),
              Elasticity::HadoopBootstrapAction.new('-m', 'mapreduce.user.classpath.first=true')
            ].each do |action|
              @jobflow.add_bootstrap_action(action)
            end
          else
            [{
              "Classification" => "core-site",
              "Properties" => {
                "io.file.buffer.size" => "65536"
              }
            },
            {
              "Classification" => "mapred-site",
              "Properties" => {
                "mapreduce.user.classpath.first" => "true"
              }
            }].each do |config|
              @jobflow.add_configuration(config)
            end
          end
        end

        # Add custom bootstrap actions
        bootstrap_actions = config[:aws][:emr][:bootstrap]
        unless bootstrap_actions.nil?
          bootstrap_actions.each do |bootstrap_action|
            @jobflow.add_bootstrap_action(Elasticity::BootstrapAction.new(bootstrap_action))
          end
        end

        # Prepare a bootstrap action based on the AMI version
        bootstrap_script_location = if @legacy
          "#{standard_assets_bucket}common/emr/snowplow-ami3-bootstrap-0.1.0.sh"
        else
          "#{standard_assets_bucket}common/emr/snowplow-ami4-bootstrap-0.2.0.sh"
        end
        cc_version = get_cc_version(config[:enrich][:versions][:spark_enrich])
        @jobflow.add_bootstrap_action(Elasticity::BootstrapAction.new(bootstrap_script_location, cc_version))

        # Install and launch HBase
        hbase = config[:aws][:emr][:software][:hbase]
        unless not hbase
          install_hbase_action = Elasticity::BootstrapAction.new("s3://#{config[:aws][:emr][:region]}.elasticmapreduce/bootstrap-actions/setup-hbase")
          @jobflow.add_bootstrap_action(install_hbase_action)

          start_hbase_step = Elasticity::CustomJarStep.new("/home/hadoop/lib/hbase-#{hbase}.jar")
          start_hbase_step.name = "Start HBase #{hbase}"
          start_hbase_step.arguments = [ 'emr.hbase.backup.Main', '--start-master' ]
          @jobflow.add_step(start_hbase_step)
        end

        # Install Lingual
        lingual = config[:aws][:emr][:software][:lingual]
        unless not lingual
          install_lingual_action = Elasticity::BootstrapAction.new("s3://files.concurrentinc.com/lingual/#{lingual}/lingual-client/install-lingual-client.sh")
          @jobflow.add_bootstrap_action(install_lingual_action)
        end

        # EMR configuration: Spark, YARN, etc
        configuration = config[:aws][:emr][:configuration]
        unless configuration.nil?
          configuration.each do |k, h|
            @jobflow.add_configuration({"Classification" => k, "Properties" => h})
          end
        end

        # Now let's add our task group if required
        tic = config[:aws][:emr][:jobflow][:task_instance_count]
        if tic > 0
          instance_group = Elasticity::InstanceGroup.new.tap { |ig|
            ig.count = tic
            ig.type  = config[:aws][:emr][:jobflow][:task_instance_type]

            tib = config[:aws][:emr][:jobflow][:task_instance_bid]
            if tib.nil?
              ig.set_on_demand_instances
            else
              ig.set_spot_instances(tib)
            end
          }

          @jobflow.set_task_instance_group(instance_group)
        end

        enrich_final_output = if enrich
          partition_by_run(csbe[:good], run_id)
        else
          csbe[:good] # Doesn't make sense to partition if enrich has already been done
        end

        if enrich

          raw_input = csbr[:processing]

          # for ndjson/urbanairship we can group by everything, just aim for the target size
          group_by = is_ua_ndjson(collector_format) ? ".*\/(\w+)\/.*" : ".*([0-9]+-[0-9]+-[0-9]+)-[0-9]+.*"

          # Create the Hadoop MR step for the file crushing
          compact_to_hdfs_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          compact_to_hdfs_step.arguments = [
            "--src"         , raw_input,
            "--dest"        , ENRICH_STEP_INPUT,
            "--s3Endpoint"  , s3_endpoint
          ] + [
            "--groupBy"     , group_by,
            "--targetSize"  , "128",
            "--outputCodec" , "lzo"
          ].select { |el|
            is_cloudfront_log(collector_format) || is_ua_ndjson(collector_format)
          }
          # uncompress events that are gzipped since this format is unsplittable and causes issues
          # downstream in the spark enrich job snowplow/snowplow#3525
          if collector_format == "clj-tomcat" then
            compact_to_hdfs_step.arguments << "--outputCodec" << "none"
          end
          compact_to_hdfs_step.name << ": Raw S3 -> Raw HDFS"
          @jobflow.add_step(compact_to_hdfs_step)

          # 2. Enrichment
          enrich_step =
            if is_spark_enrich(config[:enrich][:versions][:spark_enrich]) then
              @jobflow.add_application("Spark")
              build_spark_step(
                "Enrich Raw Events",
                assets[:enrich],
                "enrich.spark.EnrichJob",
                { :in     => glob_path(ENRICH_STEP_INPUT),
                  :good   => ENRICH_STEP_OUTPUT,
                  :bad    => partition_by_run(csbe[:bad],    run_id)
                },
                { 'input-format'    => collector_format,
                  'etl-timestamp'   => etl_tstamp,
                  'iglu-config'     => build_iglu_config_json(resolver),
                  'enrichments'     => build_enrichments_json(enrichments_array)
                }
              )
            else
              build_scalding_step(
                "Enrich Raw Events",
                assets[:enrich],
                "enrich.hadoop.EtlJob",
                { :in     => glob_path(ENRICH_STEP_INPUT),
                  :good   => ENRICH_STEP_OUTPUT,
                  :bad    => partition_by_run(csbe[:bad],    run_id),
                  :errors => partition_by_run(csbe[:errors], run_id, config[:enrich][:continue_on_unexpected_error])
                },
                { :input_format     => collector_format,
                  :etl_tstamp       => etl_tstamp,
                  :iglu_config      => build_iglu_config_json(resolver),
                  :enrichments      => build_enrichments_json(enrichments_array)
                }
              )
            end

          # Late check whether our enrichment directory is empty. We do an early check too
          csbe_good_loc = Sluice::Storage::S3::Location.new(csbe[:good])
          unless Sluice::Storage::S3::is_empty?(s3, csbe_good_loc)
            raise DirectoryNotEmptyError, "Cannot safely add enrichment step to jobflow, #{csbe_good_loc} is not empty"
          end
          @jobflow.add_step(enrich_step)

          # We need to copy our enriched events from HDFS back to S3
          copy_to_s3_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_to_s3_step.arguments = [
            "--src"        , ENRICH_STEP_OUTPUT,
            "--dest"       , enrich_final_output,
            "--srcPattern" , PARTFILE_REGEXP,
            "--s3Endpoint" , s3_endpoint
          ] + output_codec
          copy_to_s3_step.name << ": Enriched HDFS -> S3"
          @jobflow.add_step(copy_to_s3_step)

          copy_success_file_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_success_file_step.arguments = [
            "--src"        , ENRICH_STEP_OUTPUT,
            "--dest"       , enrich_final_output,
            "--srcPattern" , SUCCESS_REGEXP,
            "--s3Endpoint" , s3_endpoint
          ]
          copy_success_file_step.name << ": Enriched HDFS _SUCCESS -> S3"
          @jobflow.add_step(copy_success_file_step)
        end

        if shred

          # 3. Shredding
          shred_final_output = partition_by_run(csbs[:good], run_id)

          # If we enriched, we free some space on HDFS by deleting the raw events
          # otherwise we need to copy the enriched events back to HDFS
          if enrich
            @jobflow.add_step(get_rmr_step(ENRICH_STEP_INPUT, standard_assets_bucket))
          else
            copy_to_hdfs_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
            copy_to_hdfs_step.arguments = [
              "--src"        , enrich_final_output, # Opposite way round to normal
              "--dest"       , ENRICH_STEP_OUTPUT,
              "--srcPattern" , PARTFILE_REGEXP,
              "--s3Endpoint" , s3_endpoint
            ] # Either user doesn't want compression, or files are already compressed
            copy_to_hdfs_step.name << ": Enriched S3 -> HDFS"
            @jobflow.add_step(copy_to_hdfs_step)
          end

          shred_step =
            if is_rdb_shredder(config[:storage][:versions][:rdb_shredder]) then
              @jobflow.add_application("Spark")
              duplicate_storage_config = build_duplicate_storage_json(targets[:DUPLICATE_TRACKING], false)
              build_spark_step(
                "Shred Enriched Events",
                assets[:shred],
                "storage.spark.ShredJob",
                { :in   => glob_path(ENRICH_STEP_OUTPUT),
                  :good => SHRED_STEP_OUTPUT,
                  :bad  => partition_by_run(csbs[:bad], run_id)
                },
                {
                  'iglu-config' => build_iglu_config_json(resolver)
                }.merge(duplicate_storage_config)
              )
            else
              duplicate_storage_config = build_duplicate_storage_json(targets[:DUPLICATE_TRACKING])
              build_scalding_step(
                "Shred Enriched Events",
                assets[:shred],
                "enrich.hadoop.ShredJob",
                { :in          => glob_path(ENRICH_STEP_OUTPUT),
                  :good        => SHRED_STEP_OUTPUT,
                  :bad         => partition_by_run(csbs[:bad],    run_id),
                  :errors      => partition_by_run(csbs[:errors], run_id, config[:enrich][:continue_on_unexpected_error])
                },
                {
                  :iglu_config => build_iglu_config_json(resolver)
                }.merge(duplicate_storage_config)
              )
            end

          # Late check whether our target directory is empty
          csbs_good_loc = Sluice::Storage::S3::Location.new(csbs[:good])
          unless Sluice::Storage::S3::is_empty?(s3, csbs_good_loc)
            raise DirectoryNotEmptyError, "Cannot safely add shredding step to jobflow, #{csbs_good_loc} is not empty"
          end
          @jobflow.add_step(shred_step)

          # We need to copy our shredded types from HDFS back to S3
          copy_to_s3_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_to_s3_step.arguments = [
            "--src"        , SHRED_STEP_OUTPUT,
            "--dest"       , shred_final_output,
            "--srcPattern" , PARTFILE_REGEXP,
            "--s3Endpoint" , s3_endpoint
          ] + output_codec
          copy_to_s3_step.name << ": Shredded HDFS -> S3"
          @jobflow.add_step(copy_to_s3_step)

          copy_success_file_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_success_file_step.arguments = [
            "--src"        , SHRED_STEP_OUTPUT,
            "--dest"       , shred_final_output,
            "--srcPattern" , SUCCESS_REGEXP,
            "--s3Endpoint" , s3_endpoint
          ]
          copy_success_file_step.name << ": Shredded HDFS _SUCCESS -> S3"
          @jobflow.add_step(copy_success_file_step)
        end

        if es
          get_elasticsearch_steps(config, assets, enrich, shred, targets[:FAILED_EVENTS]).each do |step|
            @jobflow.add_step(step)
          end
        end

        if archive_raw
          # We need to copy our enriched events from HDFS back to S3
          archive_raw_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          archive_raw_step.arguments = [
            "--src"        , csbr[:processing],
            "--dest"       , partition_by_run(csbr[:archive], run_id),
            "--s3Endpoint" , s3_endpoint,
            "--deleteOnSuccess"
          ]
          archive_raw_step.name << ": Raw Staging S3 -> Raw Archive S3"
          @jobflow.add_step(archive_raw_step)
        end

        if rdb_load
          get_rdb_loader_steps(config, targets[:ENRICHED_EVENTS], resolver, assets[:loader], rdbloader_steps).each do |step|
            @jobflow.add_step(step)
          end
        end

        if archive_enriched == 'pipeline'
          archive_enriched_step = get_archive_step(csbe[:good], csbe[:archive], run_id, s3_endpoint, ": Enriched S3 -> Enriched Archive S3")
          @jobflow.add_step(archive_enriched_step)
        elsif archive_enriched == 'recover'
          latest_run_id = get_latest_run_id(s3, csbe[:good])
          archive_enriched_step = get_archive_step(csbe[:good], csbe[:archive], latest_run_id, s3_endpoint, ': Enriched S3 -> S3 Enriched Archive')
          @jobflow.add_step(archive_enriched_step)
        else    # skip
          nil
        end

        if archive_shredded == 'pipeline'
          archive_shredded_step = get_archive_step(csbs[:good], csbs[:archive], run_id, s3_endpoint, ": Shredded S3 -> Shredded Archive S3")
          @jobflow.add_step(archive_shredded_step)
        elsif archive_shredded == 'recover'
          latest_run_id = get_latest_run_id(s3, csbs[:good])
          archive_shredded_step = get_archive_step(csbs[:good], csbs[:archive], latest_run_id, s3_endpoint, ": Shredded S3 -> S3 Shredded Archive")
          @jobflow.add_step(archive_shredded_step)
        else    # skip
          nil
        end

        self
      end

      # Create one step for each Elasticsearch target for each source for that target
      #
      Contract ConfigHash, Hash, Bool, Bool, ArrayOf[Iglu::SelfDescribingJson] => ArrayOf[Elasticity::ScaldingStep]
      def get_elasticsearch_steps(config, assets, enrich, shred, failure_storages)

        # The default sources are the enriched and shredded errors generated for this run
        sources = []
        sources << partition_by_run(config[:aws][:s3][:buckets][:enriched][:bad], @run_id) if enrich
        sources << partition_by_run(config[:aws][:s3][:buckets][:shredded][:bad], @run_id) if shred

        steps = failure_storages.flat_map { |target|

          sources.map { |source|
            step = Elasticity::ScaldingStep.new(
              assets[:elasticsearch],
              "com.snowplowanalytics.snowplow.storage.hadoop.ElasticsearchJob",
              ({
                :input => source,
                :host => target.data[:host],
                :port => target.data[:port].to_s,
                :index => target.data[:index],
                :type => target.data[:type],
                :es_nodes_wan_only => target.data[:nodesWanOnly] ? "true" : "false"
              }).reject { |k, v| v.nil? }
            )
            step_name = "Errors in #{source} -> Elasticsearch: #{target.data[:name]}"
            step.name << ": #{step_name}"
            step
          }
        }

        # Wait 60 seconds before starting the first step so S3 can become consistent
        if (enrich || shred) && steps.any?
          steps[0].arguments << '--delay' << '60'
        end
        steps
      end

      # Run (and wait for) the daily ETL job.
      #
      # Throws a BootstrapFailureError if the job fails due to a bootstrap failure.
      # Throws an EmrExecutionError if the jobflow fails for any other reason.
      Contract ConfigHash => nil
      def run(config)

        snowplow_tracking_enabled = ! config[:monitoring][:snowplow].nil?

        jobflow_id = @jobflow.run
        logger.debug "EMR jobflow #{jobflow_id} started, waiting for jobflow to complete..."

        if snowplow_tracking_enabled
          Monitoring::Snowplow.parameterize(config)
          Monitoring::Snowplow.instance.track_job_started(@jobflow)
        end

        status = wait_for()

        if status.successful or status.rdb_loader_failure or status.rdb_loader_cancellation
          log_level = if status.successful
            'info'
          elsif status.rdb_loader_cancellation
            'warn'
          else
            'error'
          end
          output_rdb_loader_logs(config[:aws][:s3][:region], config[:aws][:access_key_id],
            config[:aws][:secret_access_key], log_level)
        end

        if status.successful
          logger.debug "EMR jobflow #{jobflow_id} completed successfully."
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_job_succeeded(@jobflow)
          end

        elsif status.bootstrap_failure
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_job_failed(@jobflow)
          end
          raise BootstrapFailureError, get_failure_details(jobflow_id)

        else
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_job_failed(@jobflow)
          end
          raise EmrExecutionError, get_failure_details(jobflow_id)
        end

        nil
      end

      # Fetch logs from S3 left by RDB Loader steps
      #
      # Parameters:
      # +region+:: region for logs bucket
      Contract String, String, String, String => nil
      def output_rdb_loader_logs(region, aws_access_key_id, aws_secret_key, log_level)

        s3 = Sluice::Storage::S3::new_fog_s3_from(region, aws_access_key_id, aws_secret_key)

        loc = Sluice::Storage::S3::Location.new(@rdb_loader_log_base)

        if @rdb_loader_logs.empty? or Sluice::Storage::S3::is_empty?(s3, loc)
          logger.info "No RDB Loader logs"
        else
          logger.info "RDB Loader logs"

          @rdb_loader_logs.each do |l|
            tmp = Tempfile.new("rdbloader")
            uri = URI.parse(l[1])
            bucket, key = uri.host, uri.path[1..-1]
            logger.debug "Downloading #{uri} to #{tmp.path}"
            begin
              log = s3.directories.get(bucket).files.head(key)
              Sluice::Storage::S3::download_file(s3, log, tmp)
              if log_level == 'info'
                logger.info l[0]
                logger.info tmp.read
              elsif log_level == 'warn'
                logger.warn l[0]
                logger.warn tmp.read
              else
                logger.error l[0]
                logger.error tmp.read
              end
            rescue Exception => e
              logger.error "Error while downloading RDB log #{l[1]}"
              logger.error e.message
            ensure
              tmp.close
              tmp.unlink
            end
          end
        end

        nil
      end

    private


      # Build an Elasticity RDB Loader step.
      #
      # Parameters:
      # +config+:: main Snowplow config.yml
      # +targets+:: list of Storage target config hashes
      # +resolver+:: base64-encoded Iglu resolver JSON
      # +jar+:: s3 object with RDB Loader jar
      Contract ConfigHash, ArrayOf[Iglu::SelfDescribingJson], String, String, RdbLoaderSteps => ArrayOf[Elasticity::CustomJarStep]
      def get_rdb_loader_steps(config, targets, resolver, jar, rdbloader_steps)

        # Remove credentials from config
        clean_config = deep_copy(config)
        clean_config[:aws][:access_key_id] = ""
        clean_config[:aws][:secret_access_key] = ""

        default_arguments = {
          :config      => Base64.strict_encode64(recursive_stringify_keys(clean_config).to_yaml),
          :resolver    => build_iglu_config_json(resolver)
        }

        targets.map { |target|
          name = target.data[:name]
          log_key = @rdb_loader_log_base + SecureRandom.uuid
          @rdb_loader_logs << [name, log_key]
          encoded_target = Base64.strict_encode64(target.to_json.to_json)
          arguments = [
            "--config", default_arguments[:config],
            "--resolver", default_arguments[:resolver],
            "--logkey", log_key,
            "--target", encoded_target
          ] + unless rdbloader_steps[:skip].empty?
            ["--skip", rdbloader_steps[:skip].join(",")]
          else
            []
          end + unless rdbloader_steps[:include].empty?
            ["--include", rdbloader_steps[:include].join(",")]
          else
            []
          end

          rdb_loader_step = Elasticity::CustomJarStep.new(jar)
          rdb_loader_step.arguments = arguments
          rdb_loader_step.name << ": Load #{name} Storage Target"
          rdb_loader_step
        }
      end

      # List bucket (enriched:good or shredded:good) and return latest run folder
      # Assuming, there's usually just one folder
      #
      # Parameters:
      # +s3+:: AWS S3 client
      # +s3_path+:: Full S3 path to folder
      def get_latest_run_id(s3, s3_path)
        uri = URI.parse(s3_path)
        folders = s3.directories.get(uri.host, delimiter: '/', prefix: uri.path[1..-1]).files.common_prefixes
        # each is mandatory, otherwise there'll be pagination issues if there are > 1k objects
        # cf snowplow/snowplow#3434
        run_folders = []
        folders.each { |f|
          if f.include?('run=')
            run_folders << f
          end
        }
        begin
          folder = run_folders[-1].split('/')[-1]
          folder.slice('run='.length, folder.length)
        rescue NoMethodError => _
          logger.error "No run folders in [#{s3_path}] found"
          raise UnexpectedStateError, "No run folders in [#{s3_path}] found"
        end
      end

      # Defines a S3DistCp step for archiving enriched or shred folder
      #
      # Parameters:
      # +good_path+:: shredded:good or enriched:good full S3 path
      # +archive_path+:: enriched:archive or shredded:archive full S3 path
      # +run_id_folder+:: run id foler name (2017-05-10-02-45-30, without `=run`)
      # +name+:: step description to show in EMR console
      #
      # Returns a step ready for adding to the Elasticity Jobflow.
      Contract String, String, String, String, String => Elasticity::S3DistCpStep
      def get_archive_step(good_path, archive_path, run_id_folder, s3_endpoint, name)
        archive_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
        archive_step.arguments = [
          "--src"        , partition_by_run(good_path, run_id_folder),
          "--dest"       , partition_by_run(archive_path, run_id_folder),
          "--s3Endpoint" , s3_endpoint,
          "--deleteOnSuccess"
        ]
        archive_step.name << name
        archive_step
      end


      # Defines an Elasticity Scalding step.
      #
      # Parameters:
      # +step_name+:: name of step
      # +main_class+:: Java main class to run
      # +folders+:: hash of in, good, bad, errors S3/HDFS folders
      # +extra_step_args+:: additional arguments to pass to the step
      # +targets+:: list of targets parsed from self-describing JSONs
      #
      # Returns a step ready for adding to the Elasticity Jobflow.
      Contract String, String, String, Hash, Hash => Elasticity::ScaldingStep
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

        arguments['tool.partialok'] = ''

        # Now create the Hadoop MR step for the jobflow
        scalding_step = Elasticity::ScaldingStep.new(jar, "#{JAVA_PACKAGE}.#{main_class}", arguments)
        scalding_step.name << ": #{step_name}"

        scalding_step
      end

      # Defines an Elasticity Spark step.
      #
      # Parameters:
      # +step_name+:: name of the step
      # +main_class+:: class to run
      # +folders+:: hash of input, output, bad S3/HDFS folders
      # +extra_step_args+:: additional command line arguments to pass to the step
      #
      # Returns a step read to be added to the Elasticity Jobflow.
      Contract String, String, String, Hash, Hash => Elasticity::SparkStep
      def build_spark_step(step_name, jar, main_class, folders, extra_step_args={})
        arguments = extra_step_args
          .merge({
            'input-folder'  => folders[:in],
            'output-folder' => folders[:good],
            'bad-folder'    => folders[:bad],
          })
        spark_step = Elasticity::SparkStep.new(jar, "#{JAVA_PACKAGE}.#{main_class}")
        spark_step.app_arguments = arguments
        spark_step.spark_arguments = {
          'master' => 'yarn',
          'deploy-mode' => 'cluster'
        }
        spark_step.name << ": #{step_name}"
        spark_step
      end

      # Wait for a jobflow.
      # Check its status every 5 minutes till it completes.
      #
      # Returns true if the jobflow completed without error,
      # false otherwise.
      Contract None => JobResult
      def wait_for()

        success = false
        bootstrap_failure = false
        rdb_loader_failure = false
        rdb_loader_cancellation = false

        # Loop until we can quit...
        while true do
          begin
            # Count up running tasks and failures
            statuses = @jobflow.cluster_step_status.map(&:state).inject([0, 0]) do |sum, state|
              [ sum[0] + (@@running_states.include?(state) ? 1 : 0), sum[1] + (@@failed_states.include?(state) ? 1 : 0) ]
            end

            # If no step is still running, then quit
            if statuses[0] == 0
              success = statuses[1] == 0 # True if no failures
              bootstrap_failure = EmrJob.bootstrap_failure?(@jobflow)
              rdb_loader_failure = EmrJob.rdb_loader_failure?(@jobflow.cluster_step_status)
              rdb_loader_cancellation = EmrJob.rdb_loader_cancellation?(@jobflow.cluster_step_status)
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
          rescue Errno::ETIMEDOUT => to
            logger.warn "Got connection timeout #{to}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue RestClient::InternalServerError => ise
            logger.warn "Got internal server error #{ise}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue Elasticity::ThrottlingException => te
            logger.warn "Got Elasticity throttling exception #{te}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue ArgumentError => ae
            logger.warn "Got Elasticity argument error #{ae}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue IOError => ioe
            logger.warn "Got IOError #{ioe}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue RestClient::SSLCertificateNotVerified => sce
            logger.warn "Got RestClient::SSSLCertificateNotVerified #{sce}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          end
        end

        JobResult.new(success, bootstrap_failure, rdb_loader_failure, rdb_loader_cancellation)
      end

      # Prettified string containing failure details
      # for this job flow.
      Contract String => String
      def get_failure_details(jobflow_id)

        cluster_step_status = @jobflow.cluster_step_status
        cluster_status = @jobflow.cluster_status

        [
          "EMR jobflow #{jobflow_id} failed, check Amazon EMR console and Hadoop logs for details (help: https://github.com/snowplow/snowplow/wiki/Troubleshooting-jobs-on-Elastic-MapReduce). Data files not archived.",
          "#{@jobflow.name}: #{cluster_status.state} [#{cluster_status.last_state_change_reason}] ~ #{self.class.get_elapsed_time(cluster_status.ready_at, cluster_status.ended_at)} #{self.class.get_timespan(cluster_status.ready_at, cluster_status.ended_at)}"
        ].concat(cluster_step_status
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

      # Recursively change the keys of a YAML from symbols to strings
      def recursive_stringify_keys(h)
        if h.class == [].class
          h.map {|key| recursive_stringify_keys(key)}
        elsif h.class == {}.class
          Hash[h.map {|k,v| [k.to_s, recursive_stringify_keys(v)]}]
        else
          h
        end
      end

      def deep_copy(o)
        Marshal.load(Marshal.dump(o))
      end

      # Returns true if the jobflow failed at a rdb loader step
      Contract ArrayOf[Elasticity::ClusterStepStatus] => Bool
      def self.rdb_loader_failure?(cluster_step_statuses)
        rdb_loader_failure_indicator = /Storage Target/
        cluster_step_statuses.any? { |s| s.state == 'FAILED' && !(s.name =~ rdb_loader_failure_indicator).nil? }
      end

      # Returns true if the rdb loader step was cancelled
      Contract ArrayOf[Elasticity::ClusterStepStatus] => Bool
      def self.rdb_loader_cancellation?(cluster_step_statuses)
        rdb_loader_failure_indicator = /Storage Target/
        cluster_step_statuses.any? { |s| s.state == 'CANCELLED' && !(s.name =~ rdb_loader_failure_indicator).nil? }
      end

      # Returns true if the jobflow seems to have failed due to a bootstrap failure
      Contract Elasticity::JobFlow => Bool
      def self.bootstrap_failure?(jobflow)
        bootstrap_failure_indicator = /BOOTSTRAP_FAILURE|bootstrap action|Master instance startup failed/
        jobflow.cluster_step_status.all? {|s| s.state == 'CANCELLED'} &&
          (!(jobflow.cluster_status.last_state_change_reason =~ bootstrap_failure_indicator).nil?)
      end

      Contract String, String => Elasticity::CustomJarStep
      def get_rmr_step(location, bucket)
        step = Elasticity::CustomJarStep.new("s3://#{@jobflow.region}.elasticmapreduce/libs/script-runner/script-runner.jar")
        step.arguments = ["#{bucket}common/emr/snowplow-hadoop-fs-rmr-0.1.0.sh", location]
        step.name << ": Empty Raw HDFS"
        step
      end

    end
  end
end

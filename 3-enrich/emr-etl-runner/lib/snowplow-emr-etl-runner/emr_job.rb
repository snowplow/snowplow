# Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2019 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'set'
require 'elasticity'
require 'aws-sdk-s3'
require 'awrence'
require 'json'
require 'base64'
require 'contracts'
require 'iglu-client'
require 'securerandom'
require 'tempfile'
require 'rest-client'

# Ruby class to execute Snowplow's Hive jobs against Amazon EMR
# using Elasticity (https://github.com/rslifka/elasticity).
module Snowplow
  module EmrEtlRunner
    class EmrJob

      include Contracts

      # Constants
      JAVA_PACKAGE = "com.snowplowanalytics.snowplow"
      PARTFILE_REGEXP = ".*part-.*"
      PARTFILE_GROUPBY_REGEXP = ".*(part-)\\d+-(.*)"
      ATOMIC_EVENTS_PARTFILE_GROUPBY_REGEXP = ".*\/atomic-events\/(part-)\\d+-(.*)"
      SHREDDED_TYPES_PARTFILE_GROUPBY_REGEXP = ".*\/shredded-types\/vendor=(.+)\/name=(.+)\/.+\/version=(.+)\/(part-)\\d+-(.*)"
      SHREDDED_TSV_TYPES_PARTFILE_GROUPBY_REGEXP = ".*\/shredded-tsv\/vendor=(.+)\/name=(.+)\/.+\/version=(.+)\/(part-)\\d+-(.*)"
      STREAM_ENRICH_REGEXP = ".*\.gz"
      SUCCESS_REGEXP = ".*_SUCCESS"
      STANDARD_HOSTED_ASSETS = "s3://snowplow-hosted-assets"
      ENRICH_STEP_INPUT = 'hdfs:///local/snowplow/raw-events/'
      ENRICH_STEP_OUTPUT = 'hdfs:///local/snowplow/enriched-events/'
      SHRED_STEP_OUTPUT = 'hdfs:///local/snowplow/shredded-events/'

      SHRED_JOB_WITH_PROCESSING_MANIFEST = Gem::Version.new('0.14.0-rc1')
      SHRED_JOB_WITH_TSV_OUTPUT = Gem::Version.new('0.16.0-rc1')
      RDB_LOADER_WITH_PROCESSING_MANIFEST = Gem::Version.new('0.15.0-rc4')

      AMI_4 = Gem::Version.new("4.0.0")
      AMI_5 = Gem::Version.new("5.0.0")

      # Need to understand the status of all our jobflow steps
      @@running_states = Set.new(%w(WAITING RUNNING PENDING SHUTTING_DOWN))
      @@failed_states  = Set.new(%w(FAILED CANCELLED))

      include Monitoring::Logging
      include Snowplow::EmrEtlRunner::Utils
      include Snowplow::EmrEtlRunner::S3
      include Snowplow::EmrEtlRunner::EMR

      # Initializes our wrapper for the Amazon EMR client.
      Contract Bool, Bool, Bool, Bool, Bool, Bool, Bool, Bool, ArchiveStep, ArchiveStep, ConfigHash, ArrayOf[String], String, TargetsHash, RdbLoaderSteps, Bool, String => EmrJob
      def initialize(debug, staging, enrich, staging_stream_enrich, shred, es, archive_raw, rdb_load, archive_enriched, archive_shredded, config, enrichments_array, resolver, targets, rdbloader_steps, use_persistent_jobflow, persistent_jobflow_duration)

        logger.debug "Initializing EMR jobflow"

        # Configuration
        custom_assets_bucket =
          get_hosted_assets_bucket(STANDARD_HOSTED_ASSETS, config[:aws][:s3][:buckets][:assets], config[:aws][:emr][:region])
        standard_assets_bucket =
          get_hosted_assets_bucket(STANDARD_HOSTED_ASSETS, STANDARD_HOSTED_ASSETS, config[:aws][:emr][:region])
        assets = get_assets(
          custom_assets_bucket,
          config.dig(:enrich, :versions, :spark_enrich),
          config[:storage][:versions][:rdb_shredder],
          config[:storage][:versions][:hadoop_elasticsearch],
          config[:storage][:versions][:rdb_loader])

        collector_format = config.dig(:collectors, :format)
        @run_tstamp = Time.new
        run_id = @run_tstamp.strftime("%Y-%m-%d-%H-%M-%S")
        @run_id = run_id
        @rdb_loader_log_base = config[:aws][:s3][:buckets][:log] + "rdb-loader/#{@run_id}/"
        @rdb_loader_logs = []   # pairs of target name and associated log
        etl_tstamp = (@run_tstamp.to_f * 1000).to_i.to_s
        output_codec = output_codec_from_compression_format(config.dig(:enrich, :output_compression))
        encrypted = config[:aws][:s3][:buckets][:encrypted]

        s3 = Aws::S3::Client.new(
          :access_key_id => config[:aws][:access_key_id],
          :secret_access_key => config[:aws][:secret_access_key],
          :region => config[:aws][:s3][:region])

        ami_version = Gem::Version.new(config[:aws][:emr][:ami_version])
        shredder_version = Gem::Version.new(config[:storage][:versions][:rdb_shredder])

        # Configure Elasticity with your AWS credentials
        Elasticity.configure do |c|
          c.access_key = config[:aws][:access_key_id]
          c.secret_key = config[:aws][:secret_access_key]
        end

        # Create a job flow
        @use_persistent_jobflow = use_persistent_jobflow
        @persistent_jobflow_duration_s = parse_duration(persistent_jobflow_duration)

        found_persistent_jobflow = false
        if use_persistent_jobflow
          emr = Elasticity::EMR.new(:region => config[:aws][:emr][:region])
          emr_jobflow_id = get_emr_jobflow_id(emr, config[:aws][:emr][:jobflow][:job_name])

          if emr_jobflow_id.nil?
            @jobflow = Elasticity::JobFlow.new
          else
            @jobflow = Elasticity::JobFlow.from_jobflow_id(emr_jobflow_id, config[:aws][:emr][:region])
            found_persistent_jobflow = true
          end

          @jobflow.action_on_failure = "CANCEL_AND_WAIT"
          @jobflow.keep_job_flow_alive_when_no_steps = true
        else
          @jobflow = Elasticity::JobFlow.new
        end

        # Configure
        @jobflow.name                 = config[:aws][:emr][:jobflow][:job_name]

        if ami_version < AMI_4
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

        unless config[:aws][:emr][:security_configuration].nil?
          @jobflow.security_configuration = config[:aws][:emr][:security_configuration]
        end

        @jobflow.log_uri              = config[:aws][:s3][:buckets][:log]
        @jobflow.enable_debugging     = debug
        @jobflow.visible_to_all_users = true

        @jobflow.instance_count       = config[:aws][:emr][:jobflow][:core_instance_count] + 1 # +1 for the master instance
        @jobflow.master_instance_type = config[:aws][:emr][:jobflow][:master_instance_type]
        @jobflow.slave_instance_type  = config[:aws][:emr][:jobflow][:core_instance_type]

        @jobflow.timeout = 120

        s3_endpoint = get_s3_endpoint(config[:aws][:s3][:region])
        csbr = config[:aws][:s3][:buckets][:raw]
        csbe = config[:aws][:s3][:buckets][:enriched]
        csbs = config[:aws][:s3][:buckets][:shredded]

        @pending_jobflow_steps = []

        # Clear HDFS if persistent jobflow has been found
        if found_persistent_jobflow
          submit_jobflow_step(get_rmr_step([ENRICH_STEP_INPUT, ENRICH_STEP_OUTPUT, SHRED_STEP_OUTPUT], standard_assets_bucket, "Empty Snowplow HDFS"), use_persistent_jobflow)
          submit_jobflow_step(get_hdfs_expunge_step, use_persistent_jobflow)
        end

        # staging
        if staging
          unless empty?(s3, csbr[:processing])
            raise DirectoryNotEmptyError, "Cannot safely add staging step to jobflow, #{csbr[:processing]} is not empty"
          end

          src_pattern = collector_format == 'clj-tomcat' ? '.*localhost\_access\_log.*\.txt.*' : '.+'
          src_pattern_regex = Regexp.new src_pattern
          non_empty_locs = csbr[:in].select { |l|
            not empty?(s3, l,
              lambda { |k| !(k =~ /\/$/) and !(k =~ /\$folder\$$/) and !(k =~ src_pattern_regex).nil? })
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
              if encrypted
                staging_step.arguments = staging_step.arguments + [ '--s3ServerSideEncryption' ]
              end
              staging_step.name = "[staging] s3-dist-cp: Raw #{l} -> Raw Staging S3"
              submit_jobflow_step(staging_step, use_persistent_jobflow)
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
        @jobflow.add_application("Hadoop") unless found_persistent_jobflow

        if collector_format == 'thrift'
          if @legacy
            [
              Elasticity::HadoopBootstrapAction.new('-c', 'io.file.buffer.size=65536'),
              Elasticity::HadoopBootstrapAction.new('-m', 'mapreduce.user.classpath.first=true')
            ].each do |action|
              @jobflow.add_bootstrap_action(action) unless found_persistent_jobflow
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
              @jobflow.add_configuration(config) unless found_persistent_jobflow
            end
          end
        end

        # Add custom bootstrap actions
        bootstrap_actions = config[:aws][:emr][:bootstrap]
        unless bootstrap_actions.nil?
          bootstrap_actions.each do |bootstrap_action|
            @jobflow.add_bootstrap_action(Elasticity::BootstrapAction.new(bootstrap_action)) unless found_persistent_jobflow
          end
        end

        # Prepare a bootstrap action based on the AMI version
        bootstrap_script_location = if ami_version < AMI_4
          "#{standard_assets_bucket}common/emr/snowplow-ami3-bootstrap-0.1.0.sh"
        elsif ami_version >= AMI_4 && ami_version < AMI_5
          "#{standard_assets_bucket}common/emr/snowplow-ami4-bootstrap-0.2.0.sh"
        else
          "#{standard_assets_bucket}common/emr/snowplow-ami5-bootstrap-0.1.0.sh"
        end
        cc_version = get_cc_version(config.dig(:enrich, :versions, :spark_enrich))
        @jobflow.add_bootstrap_action(Elasticity::BootstrapAction.new(bootstrap_script_location, cc_version)) unless found_persistent_jobflow

        # Install and launch HBase
        hbase = config[:aws][:emr][:software][:hbase]
        unless not hbase
          install_hbase_action = Elasticity::BootstrapAction.new("s3://#{config[:aws][:emr][:region]}.elasticmapreduce/bootstrap-actions/setup-hbase")
          @jobflow.add_bootstrap_action(install_hbase_action) unless found_persistent_jobflow

          start_hbase_step = Elasticity::CustomJarStep.new("/home/hadoop/lib/hbase-#{hbase}.jar")
          start_hbase_step.name = "Start HBase #{hbase}"
          start_hbase_step.arguments = [ 'emr.hbase.backup.Main', '--start-master' ]

          # NOTE: Presumes that HBase will remain available for a persistent cluster
          submit_jobflow_step(start_hbase_step, use_persistent_jobflow) unless found_persistent_jobflow
        end

        # Install Lingual
        lingual = config[:aws][:emr][:software][:lingual]
        unless not lingual
          install_lingual_action = Elasticity::BootstrapAction.new("s3://files.concurrentinc.com/lingual/#{lingual}/lingual-client/install-lingual-client.sh")
          @jobflow.add_bootstrap_action(install_lingual_action) unless found_persistent_jobflow
        end

        # EMR configuration: Spark, YARN, etc
        configuration = config[:aws][:emr][:configuration]
        unless configuration.nil?
          configuration.each do |k, h|
            @jobflow.add_configuration({"Classification" => k, "Properties" => h}) unless found_persistent_jobflow
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

        stream_enrich_mode = !csbe[:stream].nil?

        # Get full path when we need to move data to enrich_final_output
        # otherwise (when enriched/good is non-empty already)
        # we can list files withing folders using '*.'-regexps
        enrich_final_output = if enrich || staging_stream_enrich
          partition_by_run(csbe[:good], run_id)
        else
          csbe[:good]
        end

        if enrich

          raw_input = csbr[:processing]

          # When resuming from enrich, we need to check for emptiness of the processing bucket
          if !staging and empty?(s3, raw_input)
            raise NoDataToProcessError, "No Snowplow logs in #{raw_input}, can't resume from enrich"
          end

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
          if encrypted
            compact_to_hdfs_step.arguments = compact_to_hdfs_step.arguments + [ '--s3ServerSideEncryption' ]
          end
          compact_to_hdfs_step.name = "[enrich] s3-dist-cp: Raw S3 -> Raw HDFS"
          submit_jobflow_step(compact_to_hdfs_step, use_persistent_jobflow)

          # 2. Enrichment
          enrich_asset = if assets[:enrich].nil?
            raise ConfigError, "Cannot add enrich step as spark_enrich version is not configured"
          else
            assets[:enrich]
          end

          enrich_version = config.dig(:enrich, :versions, :spark_enrich)

          enrich_step =
            if is_spark_enrich(enrich_version) then
              @jobflow.add_application("Spark") unless found_persistent_jobflow
              build_spark_step(
                "[enrich] spark: Enrich Raw Events",
                enrich_asset,
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
                "[enrich] scalding: Enrich Raw Events",
                enrich_asset,
                "enrich.hadoop.EtlJob",
                { :in     => glob_path(ENRICH_STEP_INPUT),
                  :good   => ENRICH_STEP_OUTPUT,
                  :bad    => partition_by_run(csbe[:bad],    run_id),
                  :errors => partition_by_run(csbe[:errors], run_id, config.dig(:enrich, :continue_on_unexpected_error))
                },
                { :input_format     => collector_format,
                  :etl_tstamp       => etl_tstamp,
                  :iglu_config      => build_iglu_config_json(resolver),
                  :enrichments      => build_enrichments_json(enrichments_array)
                }
              )
            end

          # Late check whether our enrichment directory is empty. We do an early check too
          unless empty?(s3, csbe[:good])
            raise DirectoryNotEmptyError, "Cannot safely add enrichment step to jobflow, #{csbe[:good]} is not empty"
          end
          submit_jobflow_step(enrich_step, use_persistent_jobflow)

          # We need to copy our enriched events from HDFS back to S3
          copy_to_s3_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_to_s3_step.arguments = [
            "--src"       , ENRICH_STEP_OUTPUT,
            "--dest"      , enrich_final_output,
            "--groupBy"   , PARTFILE_GROUPBY_REGEXP,
            "--targetSize", "24",
            "--s3Endpoint", s3_endpoint
          ] + output_codec
          if encrypted
            copy_to_s3_step.arguments = copy_to_s3_step.arguments + [ '--s3ServerSideEncryption' ]
          end
          copy_to_s3_step.name = "[enrich] spark: Enriched HDFS -> S3"
          submit_jobflow_step(copy_to_s3_step, use_persistent_jobflow)

          copy_success_file_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_success_file_step.arguments = [
            "--src"        , ENRICH_STEP_OUTPUT,
            "--dest"       , enrich_final_output,
            "--srcPattern" , SUCCESS_REGEXP,
            "--s3Endpoint" , s3_endpoint
          ]
          if encrypted
            copy_success_file_step.arguments = copy_success_file_step.arguments + [ '--s3ServerSideEncryption' ]
          end
          copy_success_file_step.name = "[enrich] spark: Enriched HDFS _SUCCESS -> S3"
          submit_jobflow_step(copy_success_file_step, use_persistent_jobflow)
        end

        # Staging data produced by Stream Enrich
        if staging_stream_enrich
          unless empty?(s3, csbe[:good])
            raise DirectoryNotEmptyError, "Cannot safely add stream staging step to jobflow, #{csbe[:good]} is not empty"
          end

          src_pattern_regex = Regexp.new STREAM_ENRICH_REGEXP
          if empty?(s3, csbe[:stream], lambda { |k| !(k =~ /\/$/) and !(k =~ /\$folder\$$/) and !(k =~ src_pattern_regex).nil? })
            raise NoDataToProcessError, "No Snowplow enriched stream logs to process since last run"
          end

          staging_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          staging_step.arguments = [
            "--src"        , csbe[:stream],
            "--dest"       , enrich_final_output,
            "--s3Endpoint" , s3_endpoint,
            "--srcPattern" , STREAM_ENRICH_REGEXP,
            "--deleteOnSuccess"
          ]
          if encrypted
            staging_step.arguments = staging_step.arguments + [ '--s3ServerSideEncryption' ]
          end
          staging_step.name = "[staging_stream_enrich] s3-dist-cp: Stream Enriched #{csbe[:stream]} -> Enriched Staging S3"
          submit_jobflow_step(staging_step, use_persistent_jobflow)
        end

        if shred

          # 3. Shredding
          shred_final_output = partition_by_run(csbs[:good], run_id)

          # Add processing manifest if available
          processing_manifest = get_processing_manifest(targets)
          processing_manifest_shred_args =
            if not processing_manifest.nil?
              if shredder_version >= SHRED_JOB_WITH_PROCESSING_MANIFEST
                { 'processing-manifest-table' => processing_manifest, 'item-id' => shred_final_output }
              else
                {}
              end
            else
              {}
            end

          # Add target config JSON if necessary
          storage_target_shred_args = get_rdb_shredder_target(config, targets[:ENRICHED_EVENTS])

          # If we enriched, we free some space on HDFS by deleting the raw events
          # otherwise we need to copy the enriched events back to HDFS
          if enrich
            submit_jobflow_step(get_rmr_step([ENRICH_STEP_INPUT], standard_assets_bucket, "Empty Raw HDFS"), use_persistent_jobflow)
          else
            src_pattern = if stream_enrich_mode then STREAM_ENRICH_REGEXP else PARTFILE_REGEXP end

            copy_to_hdfs_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
            copy_to_hdfs_step.arguments = [
              "--src"        , enrich_final_output, # Opposite way round to normal
              "--dest"       , ENRICH_STEP_OUTPUT,
              "--srcPattern" , src_pattern,
              "--outputCodec", "none",
              "--s3Endpoint" , s3_endpoint
            ]
            if encrypted
              copy_to_hdfs_step.arguments = copy_to_hdfs_step.arguments + [ '--s3ServerSideEncryption' ]
            end
            copy_to_hdfs_step.name = "[shred] s3-dist-cp: Enriched S3 -> HDFS"
            submit_jobflow_step(copy_to_hdfs_step, use_persistent_jobflow)
          end

          shred_step =
            if is_rdb_shredder(config[:storage][:versions][:rdb_shredder]) then
              @jobflow.add_application("Spark") unless found_persistent_jobflow
              duplicate_storage_config = build_duplicate_storage_json(targets[:DUPLICATE_TRACKING], false)
              build_spark_step(
                "[shred] spark: Shred Enriched Events",
                assets[:shred],
                "storage.spark.ShredJob",
                { :in   => glob_path(ENRICH_STEP_OUTPUT),
                  :good => SHRED_STEP_OUTPUT,
                  :bad  => partition_by_run(csbs[:bad], run_id)
                },
                {
                  'iglu-config' => build_iglu_config_json(resolver)
                }.merge(duplicate_storage_config).merge(processing_manifest_shred_args).merge(storage_target_shred_args)
              )
            else
              duplicate_storage_config = build_duplicate_storage_json(targets[:DUPLICATE_TRACKING])
              build_scalding_step(
                "[shred] scalding: Shred Enriched Events",
                assets[:shred],
                "enrich.hadoop.ShredJob",
                { :in          => glob_path(ENRICH_STEP_OUTPUT),
                  :good        => SHRED_STEP_OUTPUT,
                  :bad         => partition_by_run(csbs[:bad],    run_id),
                  :errors      => partition_by_run(csbs[:errors], run_id, config.dig(:enrich, :continue_on_unexpected_error))
                },
                {
                  :iglu_config => build_iglu_config_json(resolver)
                }.merge(duplicate_storage_config)
              )
            end

          # Late check whether our target directory is empty
          unless empty?(s3, csbs[:good])
            raise DirectoryNotEmptyError, "Cannot safely add shredding step to jobflow, #{csbs[:good]} is not empty"
          end
          submit_jobflow_step(shred_step, use_persistent_jobflow)

          # We need to copy our shredded types from HDFS back to S3
          # Whether to combine the files outputted by the shred step
          consolidate_shredded_output = config[:aws][:s3][:consolidate_shredded_output]
          if consolidate_shredded_output
            copy_atomic_events_to_s3_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
            copy_atomic_events_to_s3_step.arguments = [
              "--src"       , SHRED_STEP_OUTPUT,
              "--dest"      , shred_final_output,
              "--groupBy"   , ATOMIC_EVENTS_PARTFILE_GROUPBY_REGEXP,
              "--targetSize", "24",
              "--s3Endpoint", s3_endpoint
            ] + output_codec
            if encrypted
              copy_atomic_events_to_s3_step.arguments = copy_atomic_events_to_s3_step.arguments + [ '--s3ServerSideEncryption' ]
            end
            copy_atomic_events_to_s3_step.name = "[shred] s3-dist-cp: Shredded atomic events HDFS -> S3"
            submit_jobflow_step(copy_atomic_events_to_s3_step, use_persistent_jobflow)

            # Copy shredded JSONs (pre-R32)
            copy_shredded_types_to_s3_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
            copy_shredded_types_to_s3_step.arguments = [
              "--src"       , SHRED_STEP_OUTPUT,
              "--dest"      , shred_final_output,
              "--groupBy"   , SHREDDED_TYPES_PARTFILE_GROUPBY_REGEXP,
              "--targetSize", "24",
              "--s3Endpoint", s3_endpoint
            ] + output_codec
            if encrypted
              copy_shredded_types_to_s3_step.arguments = copy_shredded_types_to_s3_step.arguments + [ '--s3ServerSideEncryption' ]
            end
            copy_shredded_types_to_s3_step.name = "[shred] s3-dist-cp: Shredded JSON types HDFS -> S3"
            submit_jobflow_step(copy_shredded_types_to_s3_step, use_persistent_jobflow)

            # Copy shredded TSVs (R32+)
            if shredder_version >= SHRED_JOB_WITH_TSV_OUTPUT
              copy_shredded_tsv_types_to_s3_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
              copy_shredded_tsv_types_to_s3_step.arguments = [
                "--src"       , SHRED_STEP_OUTPUT,
                "--dest"      , shred_final_output,
                "--groupBy"   , SHREDDED_TSV_TYPES_PARTFILE_GROUPBY_REGEXP,
                "--targetSize", "24",
                "--s3Endpoint", s3_endpoint
              ] + output_codec
              if encrypted
                copy_shredded_tsv_types_to_s3_step.arguments = copy_shredded_tsv_types_to_s3_step.arguments + [ '--s3ServerSideEncryption' ]
              end
              copy_shredded_tsv_types_to_s3_step.name = "[shred] s3-dist-cp: Shredded TSV types HDFS -> S3"
              submit_jobflow_step(copy_shredded_tsv_types_to_s3_step, use_persistent_jobflow)
            end
          else
            copy_to_s3_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
            copy_to_s3_step.arguments = [
              "--src"       , SHRED_STEP_OUTPUT,
              "--dest"      , shred_final_output,
              "--srcPattern", PARTFILE_REGEXP,
              "--s3Endpoint", s3_endpoint
            ] + output_codec
            if encrypted
              copy_to_s3_step.arguments = copy_to_s3_step.arguments + [ '--s3ServerSideEncryption' ]
            end
            copy_to_s3_step.name = "[shred] s3-dist-cp: Shredded HDFS -> S3"
            submit_jobflow_step(copy_to_s3_step, use_persistent_jobflow)
          end

          copy_success_file_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
          copy_success_file_step.arguments = [
            "--src"        , SHRED_STEP_OUTPUT,
            "--dest"       , shred_final_output,
            "--srcPattern" , SUCCESS_REGEXP,
            "--s3Endpoint" , s3_endpoint
          ]
          if encrypted
            copy_success_file_step.arguments = copy_success_file_step.arguments + [ '--s3ServerSideEncryption' ]
          end
          copy_success_file_step.name = "[shred] s3-dist-cp: Shredded HDFS _SUCCESS -> S3"
          submit_jobflow_step(copy_success_file_step, use_persistent_jobflow)
        end

        if es
          get_elasticsearch_steps(config, assets, enrich, shred, targets[:FAILED_EVENTS]).each do |step|
            submit_jobflow_step(step, use_persistent_jobflow)
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
          if encrypted
            archive_raw_step.arguments = archive_raw_step.arguments + [ '--s3ServerSideEncryption' ]
          end
          archive_raw_step.name = "[archive_raw] s3-dist-cp: Raw Staging S3 -> Raw Archive S3"
          submit_jobflow_step(archive_raw_step, use_persistent_jobflow)
        end

        if rdb_load
          rdb_loader_version = Gem::Version.new(config[:storage][:versions][:rdb_loader])
          skip_manifest = stream_enrich_mode && rdb_loader_version > RDB_LOADER_WITH_PROCESSING_MANIFEST
          get_rdb_loader_steps(config, targets[:ENRICHED_EVENTS], resolver, assets[:loader], rdbloader_steps, skip_manifest).each do |step|
            submit_jobflow_step(step, use_persistent_jobflow)
          end
        end

        if archive_enriched == 'pipeline'
          archive_enriched_step = get_archive_step(csbe[:good], csbe[:archive], run_id, s3_endpoint, "[archive_enriched] s3-dist-cp: Enriched S3 -> Enriched Archive S3", encrypted)
          submit_jobflow_step(archive_enriched_step, use_persistent_jobflow)
        elsif archive_enriched == 'recover'
          latest_run_id = get_latest_run_id(s3, csbe[:good])
          archive_enriched_step = get_archive_step(csbe[:good], csbe[:archive], latest_run_id, s3_endpoint, '[archive_enriched] s3-dist-cp: Enriched S3 -> S3 Enriched Archive', encrypted)
          submit_jobflow_step(archive_enriched_step, use_persistent_jobflow)
        else    # skip
          nil
        end

        if archive_shredded == 'pipeline'
          archive_shredded_step = get_archive_step(csbs[:good], csbs[:archive], run_id, s3_endpoint, "[archive_shredded] s3-dist-cp: Shredded S3 -> Shredded Archive S3", encrypted)
          submit_jobflow_step(archive_shredded_step, use_persistent_jobflow)
        elsif archive_shredded == 'recover'
          latest_run_id = get_latest_run_id(s3, csbs[:good], 'atomic-events')
          archive_shredded_step = get_archive_step(csbs[:good], csbs[:archive], latest_run_id, s3_endpoint, "[archive_shredded] s3-dist-cp: Shredded S3 -> S3 Shredded Archive", encrypted)
          submit_jobflow_step(archive_shredded_step, use_persistent_jobflow)
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
            step.name = "Errors in #{source} -> Elasticsearch: #{target.data[:name]}"
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
        if snowplow_tracking_enabled
          Monitoring::Snowplow.parameterize(config)
        end

        @pending_jobflow_steps.each do |jobflow_step|
          begin
            retries ||= 0
            # if the job flow is already running this triggers an HTTP call
            @jobflow.add_step(jobflow_step)
          rescue Elasticity::ThrottlingException, RestClient::RequestTimeout, RestClient::InternalServerError, RestClient::ServiceUnavailable, RestClient::SSLCertificateNotVerified => e
            if retries < 3
              retries += 1
              delay = 2 ** retries + 30
              logger.warn "Got error [#{e.message}] while trying to submit jobflow step [#{jobflow_step.name}] to jobflow [#{@jobflow.jobflow_id}]. Retrying in #{delay} seconds"
              sleep(delay)
              retry
            else
              if snowplow_tracking_enabled
                step_status = Elasticity::ClusterStepStatus.new
                step_status.name = "Add step [#{jobflow_step.name}] to jobflow [#{@jobflow.jobflow_id}]. (Error: [#{e.message}])"
                step_status.state = "FAILED"
                Monitoring::Snowplow.instance.track_single_step(step_status)
              end
              raise EmrExecutionError, "Can't add step [#{jobflow_step.name}] to jobflow [#{@jobflow.jobflow_id}] (retried 3 times). Error: [#{e.message}]."
            end
          end
        end

        jobflow_id = @jobflow.jobflow_id
        if jobflow_id.nil?
          begin
            retries ||= 0
            jobflow_id = @jobflow.run
          rescue Elasticity::ThrottlingException, RestClient::RequestTimeout, RestClient::InternalServerError, RestClient::ServiceUnavailable, RestClient::SSLCertificateNotVerified
            logger.warn "Got an error while trying to submit the jobflow"
            retries += 1
            sleep(2 ** retries + 30)
            retry if retries < 3
          end
        end
        logger.debug "EMR jobflow #{jobflow_id} started, waiting for jobflow to complete..."

        if snowplow_tracking_enabled
          Monitoring::Snowplow.instance.track_job_started(jobflow_id, cluster_status(@jobflow), cluster_step_status_for_run(@jobflow))
        end

        status = wait_for

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

        cluster_status = cluster_status(@jobflow)
        cluster_step_status_for_run = cluster_step_status_for_run(@jobflow)

        if status.successful
          logger.debug "EMR jobflow #{jobflow_id} completed successfully."
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_job_succeeded(jobflow_id, cluster_status, cluster_step_status_for_run)
          end

        elsif status.bootstrap_failure
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_job_failed(jobflow_id, cluster_status, cluster_step_status_for_run)
          end
          raise BootstrapFailureError, get_failure_details(jobflow_id, cluster_status, cluster_step_status_for_run)

        else
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_job_failed(jobflow_id, cluster_status, cluster_step_status_for_run)
          end
          raise EmrExecutionError, get_failure_details(jobflow_id, cluster_status, cluster_step_status_for_run)
        end

        if @use_persistent_jobflow and
            @persistent_jobflow_duration_s > 0 and
            cluster_status.created_at + @persistent_jobflow_duration_s < @run_tstamp
          logger.debug "EMR jobflow has expired and will be shutdown."
          begin
            retries ||= 0
            @jobflow.shutdown
          rescue Elasticity::ThrottlingException, RestClient::RequestTimeout, RestClient::InternalServerError, RestClient::ServiceUnavailable, RestClient::SSLCertificateNotVerified
            retries += 1
            sleep(2 ** retries + 30)
            retry if retries < 3
          end
        end

        nil
      end

      # Fetch logs from S3 left by RDB Loader steps
      #
      # Parameters:
      # +region+:: region for logs bucket
      Contract String, String, String, String => nil
      def output_rdb_loader_logs(region, aws_access_key_id, aws_secret_key, log_level)

        s3 = Aws::S3::Client.new(
          :access_key_id => aws_access_key_id,
          :secret_access_key => aws_secret_key,
          :region => region)

        if @rdb_loader_logs.empty? or empty?(s3, @rdb_loader_log_base)
          logger.info "No RDB Loader logs"
        else
          logger.info "RDB Loader logs"

          @rdb_loader_logs.each do |l|
            tmp = Tempfile.new("rdbloader")
            bucket, key = parse_bucket_prefix(l[1])
            logger.debug "Downloading #{l[1]} to #{tmp.path}"
            begin
              s3.get_object({
                response_target: tmp,
                bucket: bucket,
                key: key,
              })
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

      # Adds a step to the jobflow according to whether or not
      # we are using a persistent cluster.
      #
      # Parameters:
      # +jobflow_step+:: the step to add
      # +use_persistent_jobflow+:: whether a persistent jobflow should be used
      def submit_jobflow_step(jobflow_step, use_persistent_jobflow = false)
        if use_persistent_jobflow
          jobflow_step.action_on_failure = "CANCEL_AND_WAIT"
        end
        @pending_jobflow_steps << jobflow_step
      end

      # Build an Elasticity RDB Loader step.
      #
      # Parameters:
      # +config+:: main Snowplow config.yml
      # +targets+:: list of Storage target config hashes
      # +resolver+:: base64-encoded Iglu resolver JSON
      # +jar+:: s3 object with RDB Loader jar
      # +skip_manifest+:: whether load_manifest RDB Loader step should be skipped
      Contract ConfigHash, ArrayOf[Iglu::SelfDescribingJson], String, String, RdbLoaderSteps, Bool => ArrayOf[Elasticity::CustomJarStep]
      def get_rdb_loader_steps(config, targets, resolver, jar, rdbloader_steps, skip_manifest)

        # Remove credentials from config
        clean_config = deep_copy(config)
        clean_config[:aws][:access_key_id] = ""
        clean_config[:aws][:secret_access_key] = ""

        default_arguments = {
          :config      => Base64.strict_encode64(recursive_stringify_keys(clean_config).to_yaml),
          :resolver    => build_iglu_config_json(resolver)
        }

        skip_steps = if skip_manifest then rdbloader_steps[:skip] + ["load_manifest"] else rdbloader_steps[:skip] end

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
          ] + unless skip_steps.empty?
            ["--skip", skip_steps.join(",")]
          else
            []
          end + unless rdbloader_steps[:include].empty?
            ["--include", rdbloader_steps[:include].join(",")]
          else
            []
          end

          rdb_loader_step = Elasticity::CustomJarStep.new(jar)
          rdb_loader_step.arguments = arguments
          rdb_loader_step.name = "[rdb_load] Load #{name} Storage Target"
          rdb_loader_step
        }
      end

      # List bucket (enriched:good or shredded:good) and return latest run folder
      #
      # Parameters:
      # +s3+:: AWS S3 client
      # +s3_path+:: Full S3 path to folder
      # +suffix+:: Suffix to check for emptiness, atomic-events in case of shredded:good
      def get_latest_run_id(s3, s3_path, suffix = '')
        run_id_regex = /.*\/run=((\d|-)+)\/.*/
        folder = last_object_name(s3, s3_path,
            lambda { |k| !(k =~ /\$folder\$$/) and !k[run_id_regex, 1].nil? })
        run_id = folder[run_id_regex, 1]
        if run_id.nil?
          logger.error "No run folders in [#{s3_path}] found"
          raise UnexpectedStateError, "No run folders in [#{s3_path}] found"
        else
          path = File.join(s3_path, "run=#{run_id}", suffix)
          if empty?(s3, path)
            raise NoDataToProcessError, "Cannot archive #{path}, no data found"
          else
            run_id
          end
        end
      end

      # Defines a S3DistCp step for archiving enriched or shred folder
      #
      # Parameters:
      # +good_path+:: shredded:good or enriched:good full S3 path
      # +archive_path+:: enriched:archive or shredded:archive full S3 path
      # +run_id_folder+:: run id foler name (2017-05-10-02-45-30, without `=run`)
      # +name+:: step description to show in EMR console
      # +encrypted+:: whether the destination bucket is encrypted
      #
      # Returns a step ready for adding to the Elasticity Jobflow.
      Contract String, String, String, String, String, Bool => Elasticity::S3DistCpStep
      def get_archive_step(good_path, archive_path, run_id_folder, s3_endpoint, name, encrypted)
        archive_step = Elasticity::S3DistCpStep.new(legacy = @legacy)
        archive_step.arguments = [
          "--src"        , partition_by_run(good_path, run_id_folder),
          "--dest"       , partition_by_run(archive_path, run_id_folder),
          "--s3Endpoint" , s3_endpoint,
          "--deleteOnSuccess"
        ]
        if encrypted
          archive_step.arguments = archive_step.arguments + [ '--s3ServerSideEncryption' ]
        end
        archive_step.name = name
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
        scalding_step.name = step_name

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
        spark_step.name = step_name
        spark_step
      end

      # Wait for a jobflow.
      # Check its status every 5 minutes till it completes.
      #
      # Returns true if the jobflow completed without error,
      # false otherwise.
      Contract None => JobResult
      def wait_for

        success = false
        bootstrap_failure = false
        rdb_loader_failure = false
        rdb_loader_cancellation = false

        # Loop until we can quit...
        while true do
          begin
            cluster_step_status_for_run = cluster_step_status_for_run(@jobflow)

            if cluster_step_status_for_run.nil?
              logger.warn "Could not retrieve cluster status, waiting 5 minutes before checking jobflow again"
              sleep(300)
            else
              # Count up running tasks and failures
              statuses = cluster_step_status_for_run.map(&:state).inject([0, 0]) do |sum, state|
                [ sum[0] + (@@running_states.include?(state) ? 1 : 0), sum[1] + (@@failed_states.include?(state) ? 1 : 0) ]
              end

              # If no step is still running, then quit
              if statuses[0] == 0
                success = statuses[1] == 0 # True if no failures
                bootstrap_failure = EmrJob.bootstrap_failure?(@jobflow, cluster_step_status_for_run)
                rdb_loader_failure = EmrJob.rdb_loader_failure?(cluster_step_status_for_run)
                rdb_loader_cancellation = EmrJob.rdb_loader_cancellation?(cluster_step_status_for_run)
                break
              else
                # Sleep a while before we check again
                sleep(60)
              end
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
            logger.warn "Got RestClient::SSLCertificateNotVerified #{sce}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue RestClient::RequestTimeout => rt
            logger.warn "Got RestClient::RequestTimeout #{rt}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          rescue RestClient::ServiceUnavailable => su
            logger.warn "Got RestClient::ServiceUnavailable #{su}, waiting 5 minutes before checking jobflow again"
            sleep(300)
          end
        end

        JobResult.new(success, bootstrap_failure, rdb_loader_failure, rdb_loader_cancellation)
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

      # Ensures we only look at the steps submitted in this run
      # and not within prior persistent runs
      #
      # Parameters:
      # +jobflow+:: The jobflow to extract steps from
      Contract Elasticity::JobFlow => ArrayOf[Elasticity::ClusterStepStatus]
      def cluster_step_status_for_run(jobflow)
        begin
          retries ||= 0
          jobflow.cluster_step_status
            .select { |a| a.created_at >= @run_tstamp }
            .sort_by { |a| a.created_at }
        rescue Elasticity::ThrottlingException, RestClient::RequestTimeout, RestClient::InternalServerError, RestClient::ServiceUnavailable, RestClient::SSLCertificateNotVerified
          retries += 1
          sleep(2 ** retries + 30)
          retry if retries < 3
        end
      end

      Contract Elasticity::JobFlow => Elasticity::ClusterStatus
      def cluster_status(jobflow)
        begin
          retries ||= 0
          jobflow.cluster_status
        rescue Elasticity::ThrottlingException, RestClient::RequestTimeout, RestClient::InternalServerError, RestClient::ServiceUnavailable, RestClient::SSLCertificateNotVerified
          retries += 1
          sleep(2 ** retries + 30)
          retry if retries < 3
        end
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
      Contract Elasticity::JobFlow, ArrayOf[Elasticity::ClusterStepStatus] => Bool
      def self.bootstrap_failure?(jobflow, cluster_step_statuses)
        bootstrap_failure_indicator = /BOOTSTRAP_FAILURE|bootstrap action|Master instance startup failed/
        cluster_step_statuses.all? { |s| s.state == 'CANCELLED' } &&
          (!(jobflow.cluster_status.last_state_change_reason =~ bootstrap_failure_indicator).nil?)
      end

      Contract ArrayOf[String], String, String => Elasticity::CustomJarStep
      def get_rmr_step(locations, bucket, description)
        step = Elasticity::CustomJarStep.new("s3://#{@jobflow.region}.elasticmapreduce/libs/script-runner/script-runner.jar")
        step.arguments = ["#{bucket}common/emr/snowplow-hadoop-fs-rmr-0.2.0.sh"] + locations
        step.name = "[cleanup] #{description}"
        step
      end

      def get_hdfs_expunge_step
        step = Elasticity::CustomJarStep.new("command-runner.jar")
        step.arguments = %W(hdfs dfs -expunge)
        step.name = "[cleanup] Empty HDFS trash"
        step
      end

      Contract TargetsHash => Maybe[String]
      def get_processing_manifest(targets)
        targets[:ENRICHED_EVENTS].select { |t| not t.data[:processingManifest].nil? }.map { |t| t.data.dig(:processingManifest, :amazonDynamoDb, :tableName) }.first
      end

      Contract ConfigHash, ArrayOf[Iglu::SelfDescribingJson] => Hash
      def get_rdb_shredder_target(config, targets)
        supported_targets = targets.select { |target_config|
          target_config.schema.name == 'redshift_config' && target_config.schema.version.model >= 4
        }
        if Gem::Version.new(config[:storage][:versions][:rdb_shredder]) >= SHRED_JOB_WITH_TSV_OUTPUT && !supported_targets.empty?
          { 'target' => Base64.strict_encode64(supported_targets.first.to_json.to_json) }
        else
          {}
        end
      end
    end
  end
end

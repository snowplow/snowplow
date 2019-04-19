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

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2019 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'contracts'
require 'base64'
require 'iglu-client'

# Implementation of Generator for playbooks
module Snowplow
  module EmrEtlRunner
    class EmrPlaybookGenerator

      include Snowplow::EmrEtlRunner::Generator
      include Snowplow::EmrEtlRunner::Utils
      include Contracts

      STANDARD_HOSTED_ASSETS = "s3://snowplow-hosted-assets"
      ENRICH_STEP_INPUT = 'hdfs:///local/snowplow/raw-events/'
      ENRICH_STEP_OUTPUT = 'hdfs:///local/snowplow/enriched-events/'
      SHRED_STEP_OUTPUT = 'hdfs:///local/snowplow/shredded-events/'
      PART_REGEX = '.*part-.*'
      SUCCESS_REGEX = '.*_SUCCESS'

      Contract String => Iglu::SchemaKey
      def get_schema_key(version)
        Iglu::SchemaKey.parse_key("iglu:com.snowplowanalytics.dataflowrunner/PlaybookConfig/avro/#{version}")
      end

      Contract ConfigHash, Bool, Maybe[String], String, ArrayOf[String] => Hash
      def create_datum(config, debug, resume_from, resolver, enrichments)
        staging = resume_from.nil?
        enrich = resume_from.nil? or resume_from == 'enrich'
        shred = resume_from.nil? or [ 'enrich', 'shred' ].include?(resume_from)
        es = resume_from.nil? or [ 'enrich', 'shred', 'elasticsearch' ].include?(resume_from)

        {
          "region" => config[:aws][:emr][:region],
          "credentials" => {
            "accessKeyId" => config[:aws][:access_key_id],
            "secretAccessKey" => config[:aws][:secret_access_key]
          },
          "steps" =>
            get_steps(config, debug, staging, enrich, shred, es, resolver, enrichments)
        }
      end

      private

      Contract ConfigHash, Bool, Bool, Bool, Bool, Bool, String, ArrayOf[String] => ArrayOf[Hash]
      def get_steps(config, debug, staging, enrich, shred, es, resolver, enrichments)
        steps = []

        if debug
          steps << get_debugging_step(config[:aws][:emr][:region])
        end

        if config[:aws][:emr][:software][:hbase]
          steps << get_hbase_step(config[:aws][:emr][:software][:hbase])
        end

        run_tstamp = Time.new
        run_id = run_tstamp.strftime("%Y-%m-%d-%H-%M-%S")
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
        legacy = (not (config[:aws][:emr][:ami_version] =~ /^[1-3].*/).nil?)
        region = config[:aws][:emr][:region]
        s3_endpoint = get_s3_endpoint(region)

        csbe = config[:aws][:s3][:buckets][:enriched]
        enrich_final_output = enrich ? partition_by_run(csbe[:good], run_id) : csbe[:good]

        collector_format = config.dig(:collectors, :format)

        if enrich
          etl_tstamp = (run_tstamp.to_f * 1000).to_i.to_s
          steps += get_enrich_steps(config, legacy, s3_endpoint, collector_format,
            assets[:enrich], enrich_final_output, run_id, etl_tstamp, resolver,
            enrichments)
        end

        if shred
          # If we enriched, we free some space on HDFS by deleting the raw events
          # otherwise we need to copy the enriched events back to HDFS
          if enrich
            steps << get_rmr_step(region, ENRICH_STEP_INPUT, standard_assets_bucket)
          else
            steps << get_s3distcp_step(legacy, 'S3DistCp: enriched S3 -> HDFS',
              enrich_final_output, ENRICH_STEP_OUTPUT, s3_endpoint, [ '--srcPattern', PART_REGEX ])
          end
          # Late check whether our target directory is empty
          steps +=
            get_shred_steps(config, legacy, s3_endpoint, assets[:shred], run_id, resolver)
        end

        if es
          steps += get_es_steps(config, enrich, shred, assets[:elasticsearch], run_id)
        end

        steps << get_s3distcp_step(legacy, 'S3DistCp: raw S3 staging -> S3 archive',
          config[:aws][:s3][:buckets][:raw][:processing],
          partition_by_run(config[:aws][:s3][:buckets][:raw][:archive], run_id),
          s3_endpoint,
          [ '--deleteOnSuccess' ]
        )

        steps
      end

      Contract Bool, Bool, Bool, String, String, String,
        ArrayOf[String], String, String, String, String => ArrayOf[Hash]
      def get_staging_steps(enrich, shred, legacy, region, s3_endpoint, collector_format,
          csbr_in, csbr_processing, csbe_good, csbs_good, bucket)
        steps = []

        src_pattern = collector_format == 'clj-tomcat' ? '.*localhost\_access\_log.*\.txt.*' : '.+'
        csbr_in.map { |l|
          steps << get_s3distcp_step(legacy, "S3DistCp: staging of #{l}",
            l, csbr_processing, s3_endpoint, [ '--srcPattern', src_pattern, '--deleteOnSuccess' ])
        }
        steps
      end

      Contract ConfigHash, Bool, Bool, String, String => ArrayOf[Hash]
      def get_es_steps(config, enrich, shred, jar, run_id)
        elasticsearch_targets = config[:storage][:targets].select {|t| t[:type] == 'elasticsearch'}

        # The default sources are the enriched and shredded errors generated for this run
        default_sources = []
        default_sources << partition_by_run(config[:aws][:s3][:buckets][:enriched][:bad], run_id) if enrich
        default_sources << partition_by_run(config[:aws][:s3][:buckets][:shredded][:bad], run_id) if shred

        steps = elasticsearch_targets.flat_map { |target|
          sources = target[:sources] || default_sources
          sources.map { |s|
            args = []
            {
              '--input' => s,
              '--host' => target[:host],
              '--port' => target[:port].to_s,
              '--index' => target[:database],
              '--type' => target[:table],
              '--es_nodes_wan_only' => target[:es_nodes_wan_only] ? "true" : "false"
            }.reject { |k, v| v.nil? }
              .each do |k, v|
                args << k << v
              end
            step = get_custom_jar_step("Errors in #{s} -> Elasticsearch: #{target[:name]}", jar,
              [ 'com.snowplowanalytics.snowplow.storage.hadoop.ElasticsearchJob' ] + args)
            step
          }
        }

        # Wait 60 seconds before starting the first step so S3 can become consistent
        if (enrich || shred) && steps.any?
          steps[0]['arguments'] << '--delay' << '60'
        end
        steps
      end

      Contract ConfigHash, Bool, String, String, String, String => ArrayOf[Hash]
      def get_shred_steps(config, legacy, s3_endpoint, jar, run_id, resolver)
        steps = []

        csbs = config[:aws][:s3][:buckets][:shredded]
        shred_final_output = partition_by_run(csbs[:good], run_id)

        steps << get_scalding_step('Shred enriched events', jar,
          'com.snowplowanalytics.snowplow.enrich.hadoop.ShredJob',
          {
            :in     => glob_path(ENRICH_STEP_OUTPUT),
            :good   => SHRED_STEP_OUTPUT,
            :bad    => partition_by_run(csbs[:bad], run_id),
            :errors => partition_by_run(csbs[:errors], run_id, config[:enrich][:continue_on_unexpected_error])
          },
          [ '--iglu_config', Base64.strict_encode64(resolver) ]
        )

        output_codec = output_codec_from_compression_format(config.dig(:enrich, :output_compression))
        steps << get_s3distcp_step(legacy, 'S3DistCp: shredded HDFS -> S3', SHRED_STEP_OUTPUT,
          shred_final_output, s3_endpoint, [ '--srcPattern', PART_REGEX ] + output_codec)
        steps << get_s3distcp_step(legacy, 'S3DistCp: shredded HDFS _SUCCESS -> S3',
          SHRED_STEP_OUTPUT, shred_final_output, s3_endpoint, [ '--srcPattern', SUCCESS_REGEX ])

        steps
      end

      Contract ConfigHash, Bool, String, String, String,
        String, String, String, String, ArrayOf[String] => ArrayOf[Hash]
      def get_enrich_steps(config, legacy, s3_endpoint, collector_format, jar,
          enrich_final_output, run_id, etl_tstamp, resolver, enrichments)
        steps = []

        raw_input = config[:aws][:s3][:buckets][:raw][:processing]
        csbe = config[:aws][:s3][:buckets][:enriched]

        added_args = if is_cloudfront_log(collector_format) || is_ua_ndjson(collector_format)
          [
            '--groupBy', is_ua_ndjson(collector_format) ? ".*\/(\w+)\/.*" : ".*([0-9]+-[0-9]+-[0-9]+).*",
            '--targetSize', '128',
            '--outputCodec', 'lzo'
          ]
        else
          []
        end
        steps << get_s3distcp_step(legacy, 'S3DistCp: raw S3 -> HDFS',
          raw_input, ENRICH_STEP_INPUT, s3_endpoint, added_args)

        steps << get_scalding_step('Enrich raw events', jar,
          'com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob',
          {
            :in     => glob_path(ENRICH_STEP_INPUT),
            :good   => ENRICH_STEP_OUTPUT,
            :bad    => partition_by_run(csbe[:bad], run_id),
            :errors => partition_by_run(csbe[:errors], run_id, config[:enrich][:continue_on_unexpected_error])
          },
          [
            '--input_format', collector_format,
            '--etl_tstamp', etl_tstamp,
            '--iglu_config', Base64.strict_encode64(resolver),
            '--enrichments', build_enrichments_json(enrichments)
          ]
        )

        output_codec = output_codec_from_compression_format(config.dig(:enrich, :output_compression))
        steps << get_s3distcp_step(legacy, 'S3DistCp: enriched HDFS -> S3', ENRICH_STEP_OUTPUT,
          enrich_final_output, s3_endpoint, [ '--srcPattern', PART_REGEX ] + output_codec)
        steps << get_s3distcp_step(legacy, 'S3DistCp: enriched HDFS _SUCCESS -> S3',
          ENRICH_STEP_OUTPUT, enrich_final_output, s3_endpoint, [ '--srcPattern', SUCCESS_REGEX ])

        steps
      end

      Contract String => Hash
      def get_debugging_step(region)
        get_custom_jar_step('Setup Hadoop debugging',
          "s3://#{region}.elasticmapreduce/libs/script-runner/script-runner.jar",
          [ "s3://#{region}.elasticmapreduce/libs/state-pusher/0.1/fetch" ]
        )
      end

      Contract String, String, String, Hash, ArrayOf[String] => Hash
      def get_scalding_step(name, jar, main_class, folders, args=[])
        added_args = [main_class, '--hdfs', ] + args
        {
          "--input_folder" => folders[:in],
          "--output_folder" => folders[:good],
          "--bad_rows_folder" => folders[:bad],
          "--exceptions_folder" => folders[:errors]
        }.reject { |k, v| v.nil? }
          .each do |k, v|
            added_args << k << v
          end
        get_custom_jar_step(name, jar, added_args)
      end

      Contract Bool, String, String, String, String, ArrayOf[String] => Hash
      def get_s3distcp_step(legacy, name, src, dest, endpoint, args=[])
        jar = if legacy
          '/home/hadoop/lib/emr-s3distcp-1.0.jar'
        else
          '/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar'
        end
        get_custom_jar_step(name, jar, [
            "--src", src,
            "--dest", dest,
            "--s3Endpoint", endpoint
          ] + args
        )
      end

      Contract String, String => Hash
      def get_hbase_step(hbase_version)
        get_custom_jar_step(
          "Start HBase #{hbase_version}",
          "/home/hadoop/lib/hbase-#{hbase_version}.jar",
          [ "emr.hbase.backup.Main", "--start-master" ]
        )
      end

      Contract String, String, String => Hash
      def get_rmr_step(region, location, bucket)
        get_script_step(region, "Recursively removing content from #{location}",
          "#{bucket}common/emr/snowplow-hadoop-fs-rmr.sh", [ location ])
      end

      Contract String, String, String, ArrayOf[String] => Hash
      def get_script_step(region, name, script, args=[])
        get_custom_jar_step(name,
          "s3://#{region}.elasticmapreduce/libs/script-runner/script-runner.jar", [script] + args)
      end

      Contract String, String, ArrayOf[String] => Hash
      def get_custom_jar_step(name, jar, args=[])
        {
          "type" => "CUSTOM_JAR",
          "name" => name,
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => jar,
          "arguments" => args
        }
      end

    end
  end
end

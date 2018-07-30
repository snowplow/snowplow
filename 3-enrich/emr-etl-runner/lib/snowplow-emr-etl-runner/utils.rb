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

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2017 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'contracts'

# Module with diverse utilities dealing with a few quirks in EmrEtlRunner
module Snowplow
  module EmrEtlRunner
    module Utils

      include Contracts

      # Builds the region-appropriate bucket name for Snowplow's hosted assets.
      # Has to be region-specific because of https://github.com/boto/botocore/issues/424
      #
      # Parameters:
      # +standard_bucket+:: Snowplow's hosted asset bucket
      # +bucket+:: the specified hosted assets bucket
      # +region+:: the AWS region to source hosted assets from
      Contract String, String, String => String
      def get_hosted_assets_bucket(standard_bucket, bucket, region)
        bucket = bucket.chomp('/')
        suffix = if !bucket.eql? standard_bucket or region.eql? "eu-west-1" then "" else "-#{region}" end
        "#{bucket}#{suffix}/"
      end

      # Get commons-codec version required by Spark Enrich for further replace
      # See: https://github.com/snowplow/snowplow/issues/2735
      Contract Maybe[String] => String
      def get_cc_version(she_version)
        if she_version.nil?
          "1.10"
        else
          she_version_normalized = Gem::Version.new(she_version)
          if she_version_normalized > Gem::Version.new("1.8.0")
            "1.10"
          else
            "1.5"
          end
        end
      end

      # Does this collector format represent CloudFront access logs?
      # Parameters:
      # +collector_format+:: collector format from config.yml, nil in case of stream mode
      Contract Maybe[String] => Bool
      def is_cloudfront_log(collector_format)
        if collector_format.nil?
          false
        else
          collector_format == "cloudfront" or
            collector_format.start_with?("tsv/com.amazon.aws.cloudfront/")
        end
      end

      # Does this collector format represent ndjson/urbanairship?
      # Parameters:
      # +collector_format+:: collector format from config.yml, nil in case of stream mode
      Contract Maybe[String] => Bool
      def is_ua_ndjson(collector_format)
        /^ndjson\/com\.urbanairship\.connect\/.+$/ === collector_format
      end

      # Returns the S3 endpoint for a given S3 region
      Contract String => String
      def get_s3_endpoint(s3_region)
        if s3_region == "us-east-1"
          "s3.amazonaws.com"
        else
          "s3-#{s3_region}.amazonaws.com"
        end
      end

      # Retrieves the paths of hadoop enrich, hadoop shred and hadoop elasticsearch
      Contract String, Maybe[String], String, String, String => AssetsHash
      def get_assets(assets_bucket, spark_enrich_version, rds_version, hadoop_elasticsearch_version, rdl_version)
        enrich_path_middle = if spark_enrich_version.nil?
          nil
        elsif is_spark_enrich(spark_enrich_version)
          'spark-enrich/snowplow-spark-enrich'
        else
          spark_enrich_version[0] == '0' ? 'hadoop-etl/snowplow-hadoop-etl' : 'scala-hadoop-enrich/snowplow-hadoop-enrich'
        end
        shred_path = if is_rdb_shredder(rds_version) then
          '4-storage/rdb-shredder/snowplow-rdb-shredder-'
        else
          '3-enrich/scala-hadoop-shred/snowplow-hadoop-shred-'
        end
        enrich_final = if enrich_path_middle.nil? then nil else "#{assets_bucket}3-enrich/#{enrich_path_middle}-#{spark_enrich_version}.jar" end
        {
          :enrich   => enrich_final,
          :shred    => "#{assets_bucket}#{shred_path}#{rds_version}.jar",
          :loader   => "#{assets_bucket}4-storage/rdb-loader/snowplow-rdb-loader-#{rdl_version}.jar",
          :elasticsearch => "#{assets_bucket}4-storage/hadoop-elasticsearch-sink/hadoop-elasticsearch-sink-#{hadoop_elasticsearch_version}.jar",
        }
      end

      # Check if the supplied shred version relates to the rdb-shredder or the
      # legacy scala-hadoop-shred.
      #
      # Parameters:
      # +shred_version+:: the specified shred version
      Contract String => Bool
      def is_rdb_shredder(shred_version)
        version = shred_version.split('.').map { |v| v.to_i }
        unless version.length == 3
          raise ArgumentError, 'The shred job version could not be parsed'
        end
        version[1] >= 12
      end

      # Check if the supplied enrich version relates to spark enrich or the legacy
      # scala-hadoop-enrich.
      #
      # Parameters:
      # +enrich_version+:: the specified enrich version
      Contract Maybe[String] => Bool
      def is_spark_enrich(enrich_version)
        if enrich_version.nil?
          false
        else
          version = enrich_version.split('.').map { |v| v.to_i }
          unless version.length == 3
            raise ArgumentError, 'The enrich job version could not be parsed'
          end
          version[0] >= 1 && version[1] >= 9
        end
      end

      # Returns a base64-encoded JSON containing an array of enrichment JSONs
      Contract ArrayOf[String] => String
      def build_enrichments_json(enrichments_array)
        enrichments_json_data = enrichments_array.map {|e| JSON.parse(e)}
        enrichments_json = {
          'schema' => 'iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0',
          'data'   => enrichments_json_data
        }

        Base64.strict_encode64(enrichments_json.to_json)
      end

      Contract String => String
      def build_iglu_config_json(resolver)
        Base64.strict_encode64(resolver)
      end

      Contract Maybe[Iglu::SelfDescribingJson], Bool => Hash
      def build_duplicate_storage_json(target, snake_case=true)
        if target.nil?
          {}
        else
          encoded = Base64.strict_encode64(target.to_json.to_json)
          if snake_case
            { :duplicate_storage_config => encoded }
          else
            { 'duplicate-storage-config' => encoded }
          end
        end

      end

      # We need to partition our output buckets by run ID, buckets already have trailing slashes
      #
      # Parameters:
      # +folder+:: the folder to append a run ID folder to
      # +run_id+:: the run ID to append
      # +retain+:: set to false if this folder should be nillified
      Contract Maybe[String], String, Bool => Maybe[String]
      def partition_by_run(folder, run_id, retain=true)
        unless folder.nil?
          "#{folder}run=#{run_id}/" if retain
        else
          nil
        end
      end

      # Converts the output_compression configuration field to
      Contract Maybe[String] => ArrayOf[String]
      def output_codec_from_compression_format(compression_format)
        # those are the supported compression codecs
        if not compression_format.nil? and [ 'gzip', 'gz', 'lzo', 'snappy' ].include?(compression_format.downcase)
          downcased = compression_format.downcase
          format = [ 'gzip', 'gz' ].include?(downcased) ? 'gz' : downcased
          [ '--outputCodec', format ]
        else
          []
        end
      end

      # Adds a match all glob to the end of the path
      Contract String => String
      def glob_path(path)
        path = path.chomp('/')
        path.end_with?('/*') ? path : "#{path}/*"
      end

    end
  end
end

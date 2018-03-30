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
require 'iglu-client'

# Implementation of Generator for emr clusters
module Snowplow
  module EmrEtlRunner
    class EmrConfigGenerator

      include Snowplow::EmrEtlRunner::Generator
      include Snowplow::EmrEtlRunner::Utils
      include Contracts

      STANDARD_HOSTED_ASSETS = "s3://snowplow-hosted-assets"

      AMI_4 = Gem::Version.new("4.0.0")
      AMI_5 = Gem::Version.new("5.0.0")

      Contract String => Iglu::SchemaKey
      def get_schema_key(version)
        Iglu::SchemaKey.parse_key("iglu:com.snowplowanalytics.dataflowrunner/ClusterConfig/avro/#{version}")
      end

      Contract ConfigHash, Bool, Maybe[String], String, ArrayOf[String] => Hash
      def create_datum(config, debug=false, resume_from=nil, resolver='', enrichments=[])
        ami_version = Gem::Version.new(config[:aws][:emr][:ami_version])
        region = config[:aws][:emr][:region]

        {
          "name" => config[:aws][:emr][:jobflow][:job_name],
          "logUri" => config[:aws][:s3][:buckets][:log],
          "region" => region,
          "credentials" => {
            "accessKeyId" => config[:aws][:access_key_id],
            "secretAccessKey" => config[:aws][:secret_access_key]
          },
          "roles" => {
            "jobflow" => config[:aws][:emr][:jobflow_role],
            "service" => config[:aws][:emr][:service_role]
          },
          "ec2" => {
            "amiVersion" => config[:aws][:emr][:ami_version],
            "keyName" => config[:aws][:emr][:ec2_key_name],
            "location" => get_location_hash(
              config[:aws][:emr][:ec2_subnet_id], config[:aws][:emr][:placement]),
            "instances" => {
              "master" => { "type" => config[:aws][:emr][:jobflow][:master_instance_type] },
              "core" => {
                "type" => config[:aws][:emr][:jobflow][:core_instance_type],
                "count" => config[:aws][:emr][:jobflow][:core_instance_count],
                "ebsConfiguration" =>
                  get_ebs_configuration(config[:aws][:emr][:jobflow][:core_instance_ebs])
              },
              "task" => {
                "type" => config[:aws][:emr][:jobflow][:task_instance_type],
                "count" => config[:aws][:emr][:jobflow][:task_instance_count],
                "bid" => config[:aws][:emr][:jobflow][:task_instance_bid].to_s
              }
            }
          },
          "tags" => get_tags(config[:monitoring][:tags]),
          "bootstrapActionConfigs" => get_bootstrap_actions(
            config[:aws][:emr][:bootstrap],
            config.dig(:collectors, :format),
            ami_version,
            region,
            config[:enrich][:versions][:spark_enrich]
          ),
          "configurations" => get_configurations(ami_version),
          "applications" => ["Hadoop", "Spark"]
        }
      end

      private

      Contract Hash => ArrayOf[Hash]
      def get_tags(tags)
        ts = tags.map do |k, v|
          { "key" => k.to_s, "value" => v.to_s }
        end
        ts.to_a
      end

      Contract Maybe[String], Maybe[String] => Hash
      def get_location_hash(subnet, placement)
        if subnet.nil?
          { "classic" => { "availabilityZone" => placement } }
        else
          { "vpc" => { "subnetId" => subnet } }
        end
      end

      Contract Gem::Version => ArrayOf[Hash]
      def get_configurations(ami_version)
        if ami_version < AMI_4
          []
        else
          [
            {
              "classification" => "core-site",
              "properties" => { "io.file.buffer.size" => "65536" }
            },
            {
              "classification" => "mapred-site",
              "properties" => { "mapreduce.user.classpath.first" => "true" }
            }
          ]
        end
      end

      Contract Maybe[Hash] => Hash
      def get_ebs_configuration(ebs_config)
        if ebs_config.nil?
          {}
        else
          {
            "ebsOptimized" => ebs_config[:ebs_optimized].nil? ? true : ebs_config[:ebs_optimized],
            "ebsBlockDeviceConfigs" => [
              {
                "volumesPerInstance" => 1,
                "volumeSpecification" => {
                  "iops" => ebs_config[:volume_type] == "io1" ? ebs_config[:volume_iops] : 1,
                  "sizeInGB" => ebs_config[:volume_size],
                  "volumeType" => ebs_config[:volume_type]
                }
              }
            ]
          }
        end
      end

      Contract ArrayOf[Hash], Maybe[String], Gem::Version, String, String => ArrayOf[Hash]
      def get_bootstrap_actions(actions, collector_format, ami_version, region, enrich_version)
        bs_actions = []
        bs_actions += actions
        if collector_format == 'thrift' && ami_version < AMI_4
          bs_actions += [
            get_action("Hadoop bootstrap action (buffer size)",
              "s3n://elasticmapreduce/bootstrap-actions/configure-hadoop",
              [ "-c", "io.file.buffer.size=65536" ]
            ),
            get_action("Hadoop bootstrap action (user cp first)",
              "s3n://elasticmapreduce/bootstrap-actions/configure-hadoop",
              [ "-m", "mapreduce.user.classpath.first=true" ]
            )
          ]
        else
        end
        bs_actions << get_ami_action(ami_version, region, enrich_version)
        bs_actions
      end

      Contract String => Hash
      def get_lingual_action(lingual_version)
        get_action("Bootstrap action (installing Lingual)",
          "s3://files.concurrentinc.com/lingual/#{lingual_version}/lingual-client/install-lingual-client.sh")
      end

      Contract String => Hash
      def get_hbase_action(region)
        get_action("Bootstrap action (installing HBase)",
          "s3://#{region}.elasticmapreduce/bootstrap-actions/setup-hbase")
      end

      Contract Gem::Version, String, String => Hash
      def get_ami_action(ami_version, region, enrich_version)
        standard_assets_bucket =
          get_hosted_assets_bucket(STANDARD_HOSTED_ASSETS, STANDARD_HOSTED_ASSETS, region)
        bootstrap_script_location = if ami_version < AMI_4
          "#{standard_assets_bucket}common/emr/snowplow-ami3-bootstrap-0.1.0.sh"
        elsif ami_version >= AMI_4 && ami_version < AMI_5
          "#{standard_assets_bucket}common/emr/snowplow-ami4-bootstrap-0.2.0.sh"
        else
          "#{standard_assets_bucket}common/emr/snowplow-ami5-bootstrap-0.1.0-rc1.sh"
        end
        cc_version = get_cc_version(enrich_version)
        unless cc_version.nil?
          get_action("Bootstrap action (ami bootstrap script)",
            bootstrap_script_location, [ cc_version ])
        end
      end

      Contract String, String, ArrayOf[String] => Hash
      def get_action(name, path, args=[])
        {
          "name" => name,
          "scriptBootstrapAction" => {
            "path" => path,
            "args" => args
          }
        }
      end
    end
  end
end

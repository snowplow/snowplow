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

require 'date'
require 'sluice'

require 'contracts'
include Contracts

# Config module to hold functions related to CLI argument parsing
# and config file reading to support the daily ETL job.
module SnowPlow
  module EmrEtlRunner
    class Runner

      attr_reader :args,
                  :config,
                  :assets,
                  :run_id

      # Initialize the class.
      #
      # Parameters:
      # +args_config+:: the hash of runtime arguments and
      #                 configuration options
      Contract ArgsConfigHash => Runner
      def initialize(args_config)

        @args = args_config[:args]
        @config = args_config[:config]

        # We have to rename some config fields for Elasticity - and make a manual adjustment
        @config[:emr][:jobflow][:slave_instance_type] = @config[:emr][:jobflow][:core_instance_type]
        @config[:emr][:jobflow][:instance_count] = @config[:emr][:jobflow][:core_instance_count] + 1 # +1 for the master instance
        @config[:emr][:jobflow].delete_if { |k, _|
          k.to_s.start_with?("core_")
        }

        @assets = get_assets(@config[:s3][:assets], @config[:etl][:hadoop_etl_version])

        # Generate our run ID: based on the time now
        @run_id = Time.new.strftime("%Y-%m-%d-%H-%M-%S")

        # Now let's handle the enrichments.
        # TODO: let's fix this up.
        anon_octets = if @config[:enrichments][:anon_ip][:enabled]
                        @config[:enrichments][:anon_ip][:anon_octets].to_s
                      else
                        '0' # Anonymize 0 quartets == anonymization disabled
                      end
        @config[:enrichments][:anon_ip_octets] = anon_octets

        self
      end

    private

      Contract String, String => AssetsHash
      def get_assets(assets_bucket, hadoop_etl_version)

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

    end
  end
end
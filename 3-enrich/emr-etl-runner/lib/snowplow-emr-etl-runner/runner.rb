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

require 'contracts'
include Contracts

module Snowplow
  module EmrEtlRunner
    class Runner

      # Supported options
      @@collector_formats = Set.new(%w(cloudfront clj-tomcat))
      @@skip_options = Set.new(%w(staging s3distcp emr shred archive))

      include Logging

      # Initialize the class.
      Contract ArgsHash, ConfigHash, ArrayOf[String] => Runner
      def initialize(args, config, enrichments_array)

        # Let's set our logging level immediately
        Logging::set_level config[:logging][:level]

        @args = args
        @config = validate_and_coalesce(args, config)
        @enrichments_array = enrichments_array
        
        self
      end

      # Our core flow
      Contract None => nil
      def run

        # Now our core flow
        unless @args[:skip].include?('staging')
          unless S3Tasks.stage_logs_for_emr(@args, @config)
            logger.info "No Snowplow logs to process since last run, exiting"
            exit 0
          end
        end

        unless @args[:skip].include?('emr')
          shred = not(@args[:skip].include?('shred'))
          s3distcp = not(@args[:skip].include?('s3distcp'))
          job = EmrJob.new(@args[:debug], shred, s3distcp, @config, @enrichments_array)
          job.run()
        end

        unless @args[:skip].include?('archive')
          S3Tasks.archive_logs(@config)
        end

        logger.info "Completed successfully"
        nil
      end

      # Adds trailing slashes to all non-nil bucket names in the hash
      Contract BucketHash => BucketHash
      def add_trailing_slashes(bucketsHash)
        with_slashes_added = {}
        for k0 in bucketsHash.keys
          if bucketsHash[k0].class == ''.class
            with_slashes_added[k0] = Sluice::Storage::trail_slash(bucketsHash[k0])
          elsif bucketsHash[k0].class == {}.class
            y = {}
            for k1 in bucketsHash[k0].keys
              y[k1] = bucketsHash[k0][k1].nil? ? nil : Sluice::Storage::trail_slash(bucketsHash[k0][k1])
            end
            with_slashes_added[k0] = y
          else
            with_slashes_added[k0] = nil
          end
        end

        with_slashes_added
      end

      # Validate our arguments against the configuration Hash
      # Make updates to the configuration Hash based on the
      # arguments
      Contract ArgsHash, ConfigHash => ConfigHash
      def validate_and_coalesce(args, config)

        # Check our skip argument
        args[:skip].each { |opt|
          unless @@skip_options.include?(opt)
            raise ConfigError, "Invalid option: skip can be 'staging', 'emr' or 'archive', not '#{opt}'"
          end
        }

        # Check that start is before end, if both set
        if !args[:start].nil? and !args[:end].nil?
          if args[:start] > args[:end]
            raise ConfigError, "Invalid options: end date '#{_end}' is before start date '#{start}'"
          end
        end

        # Validate the collector format
        unless @@collector_formats.include?(config[:etl][:collector_format]) 
          raise ConfigError, "collector_format '%s' not supported" % config[:etl][:collector_format]
        end

        # Currently we only support start/end times for the CloudFront collector format. See #120 for details
        unless config[:etl][:collector_format] == 'cloudfront' or (args[:start].nil? and args[:end].nil?)
          raise ConfigError, "--start and --end date arguments are only supported if collector_format is 'cloudfront'"
        end

        unless args[:process_bucket].nil?
          config[:s3][:buckets][:processing] = args[:process_bucket]
        end

        # Add trailing slashes if needed to the non-nil buckets
        config[:s3][:buckets] = add_trailing_slashes(config[:s3][:buckets])

        config
      end

    end
  end
end

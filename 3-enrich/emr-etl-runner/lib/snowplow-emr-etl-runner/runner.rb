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
# Copyright:: Copyright (c) 2012-2017 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'contracts'
require 'iglu-client'
require 'json-schema'

module Snowplow
  module EmrEtlRunner
    class Runner

      include Contracts

      # Supported options
      @@collector_format_regex = /^(?:cloudfront|clj-tomcat|thrift|(?:json\/.+\/.+)|(?:tsv\/.+\/.+)|(?:ndjson\/.+\/.+))$/
      @@skip_options = Set.new(%w(staging s3distcp emr enrich shred elasticsearch archive_raw analyze archive_enriched rdb_load))
      @@include_options = Set.new(%w(vacuum))
      @@storage_targets = Set.new(%w(redshift_config postgresql_config elastic_config amazon_dynamodb_config))

      include Monitoring::Logging

      # Initialize the class.
      Contract ArgsHash, ConfigHash, ArrayOf[String], String, ArrayOf[JsonFileHash] => Runner
      def initialize(args, config, enrichments_array, resolver, targets_array)

        # Let's set our logging level immediately
        Monitoring::Logging::set_level config[:monitoring][:logging][:level]

        @args = args
        @config = validate_and_coalesce(args, config)
        @enrichments_array = enrichments_array
        @resolver_config = resolver
        resolver_config_json = JSON.parse(@resolver_config, {:symbolize_names => true})
        @resolver = Iglu::Resolver.parse(resolver_config_json)
        @targets = group_targets(validate_targets(targets_array))
        self
      end

      # Our core flow
      Contract None => nil
      def run

        # Now our core flow
        unless @args[:skip].include?('staging')
          unless S3Tasks.stage_logs_for_emr(@args, @config)
            raise NoDataToProcessError, "No Snowplow logs to process since last run"
          end
        end

        unless @args[:skip].include?('emr')
          enrich = not(@args[:skip].include?('enrich'))
          shred = not(@args[:skip].include?('shred'))
          s3distcp = not(@args[:skip].include?('s3distcp'))
          elasticsearch = not(@args[:skip].include?('elasticsearch'))
          archive_raw = not(@args[:skip].include?('archive_raw'))
          rdb_load = not(@args[:skip].include?('rdb_load'))

          archive_enriched = if @args[:skip].include?('archive_enriched')
            'skip'
          elsif enrich
            'pipeline'
          else
            'recover'
          end

          # Keep relaunching the job until it succeeds or fails for a reason other than a bootstrap failure
          tries_left = @config[:aws][:emr][:bootstrap_failure_tries]
          rdbloader_steps = get_rdbloader_steps()
          while true
            begin
              tries_left -= 1
              job = EmrJob.new(@args[:debug], enrich, shred, elasticsearch, s3distcp, archive_raw, rdb_load, archive_enriched, @config, @enrichments_array, @resolver_config, @targets, rdbloader_steps)
              job.run(@config)
              break
            rescue BootstrapFailureError => bfe
              logger.warn "Job failed. #{tries_left} tries left..."
              if tries_left > 0
                # Random timeout between 0 and 10 minutes
                bootstrap_timeout = rand(1..600)
                logger.warn("Bootstrap failure detected, retrying in #{bootstrap_timeout} seconds...")
                sleep(bootstrap_timeout)
              else
                raise
              end
            end
          end
        end

        logger.info "Completed successfully"
        nil
      end

      # Adds trailing slashes to all non-nil bucket names in the hash
      def self.add_trailing_slashes(bucketData)
        if bucketData.class == ''.class
          Sluice::Storage::trail_slash(bucketData)
        elsif bucketData.class == {}.class
          bucketData.each {|k,v| add_trailing_slashes(v)}
        elsif bucketData.class == [].class
          bucketData.each {|b| add_trailing_slashes(b)}
        end
      end

      Contract nil => RdbLoaderSteps
      def get_rdbloader_steps()
        steps = {
          :skip => [],
          :include => []
        }

        if @args[:skip].include?("analyze")
          steps[:skip] << "analyze"
        end

        if @args[:skip].include?("shred")
          steps[:skip] << "shred"
        end

        if @args[:include].include?("vacuum")
          steps[:include] << "vacuum"
        end

        steps
      end

      # Validate our arguments against the configuration Hash
      # Make updates to the configuration Hash based on the
      # arguments
      Contract ArgsHash, ConfigHash => ConfigHash
      def validate_and_coalesce(args, config)

        # Check our skip argument
        args[:skip].each { |opt|
          unless @@skip_options.include?(opt)
            raise ConfigError, "Invalid option: skip can be 'staging', 'emr', 'enrich', 'shred', 'elasticsearch', 'rdb_load', 'archive_raw' or 'analyze' not '#{opt}'"
          end
        }

        # Check include argument
        args[:include].each { |opt|
          unless @@include_options.include?(opt)
            raise ConfigError, "Invalid option: include can be 'vacuum' not '#{opt}'"
          end
        }

        # Check that start is before end, if both set
        if !args[:start].nil? and !args[:end].nil?
          if args[:start] > args[:end]
            raise ConfigError, "Invalid options: end date '#{args[:end]}' is before start date '#{args[:start]}'"
          end
        end

        input_collector_format = config[:collectors][:format]

        # Validate the collector format
        unless input_collector_format =~ @@collector_format_regex
          raise ConfigError, "collector_format '%s' not supported" % input_collector_format
        end

        if input_collector_format == 'thrift'
          if args[:skip].include?('s3distcp')
            raise ConfigError, "Cannot process Thrift events with --skip s3distcp"
          end
          if config[:aws][:emr][:ami_version].start_with?('2')
            raise ConfigError, "Cannot process Thrift events with AMI version 2.x.x"
          end
        end

        # Currently we only support start/end times for the CloudFront collector format. See #120 for details
        unless config[:collectors][:format] == 'cloudfront' or (args[:start].nil? and args[:end].nil?)
          raise ConfigError, "--start and --end date arguments are only supported if collector_format is 'cloudfront'"
        end

        # We can't process enrich and process shred
        unless args[:process_enrich_location].nil? or args[:process_shred_location].nil?
          raise ConfigError, "Cannot process enrich and process shred, choose one"
        end
        unless args[:process_enrich_location].nil?
          config[:aws][:s3][:buckets][:raw][:processing] = args[:process_enrich_location]
        end
        unless args[:process_shred_location].nil?
          config[:aws][:s3][:buckets][:enriched][:good] = args[:process_shred_location]
        end

        # Add trailing slashes if needed to the non-nil buckets
        config[:aws][:s3][:buckets] = Runner.add_trailing_slashes(config[:aws][:s3][:buckets])

        config
      end

      # Validate array of self-describing JSONs
      Contract ArrayOf[JsonFileHash] => ArrayOf[Iglu::SelfDescribingJson]
      def validate_targets(targets)
        targets.map do |j|
          begin
            self_describing_json = Iglu::SelfDescribingJson.parse_json(j[:json])
            self_describing_json.validate(@resolver)
            j[:json] = self_describing_json
            j
          rescue JSON::ParserError, JSON::Schema::ValidationError => e
            print "Error in [#{j[:file]}] "
            puts e.message
            throw e
            abort("Shutting down")
          end
        end.map do |j|
          target_schema = j[:json].schema
          target = target_schema.name
          unless @@storage_targets.include?(target)
            print "Error in [#{j[:file]}] "
            puts "EmrEtlRunner doesn't support storage target configuration [#{target}] (schema [#{target_schema.as_uri}])"
            puts "Possible options are: #{@@storage_targets.to_a.join(', ')}"
            abort("Shutting down")
          end
          j[:json]
        end
      end

      # Build Hash with some storage target for each purpose
      Contract ArrayOf[Iglu::SelfDescribingJson] => TargetsHash
      def group_targets(targets)
        empty_targets = { :DUPLICATE_TRACKING => nil, :FAILED_EVENTS => [], :ENRICHED_EVENTS => [] }

        loaded_targets = targets.group_by { |t| t.data[:purpose] }.map { |purpose, targets|
          if targets.length == 0 && purpose.to_sym == :DUPLICATE_TRACKING
            [purpose.to_sym, nil]
          elsif targets.length == 0
            [purpose.to_sym, []]
          elsif purpose.to_sym == :DUPLICATE_TRACKING
            [purpose.to_sym, targets[0]]
          else
            [purpose.to_sym, targets]
          end
        }.to_h

        empty_targets.merge(loaded_targets)
      end
    end
  end
end

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
      @@storage_targets = Set.new(%w(redshift_config postgresql_config elastic_config amazon_dynamodb_config))

      include Monitoring::Logging

      # Decide what steps should be submitted to EMR job
      Contract ArrayOf[String], Maybe[String], Maybe[String] => EmrSteps
      def self.get_steps(skips, resume, enriched_stream)
        {
          :staging => (enriched_stream.nil? and resume.nil? and not skips.include?('staging')),
          :enrich => (enriched_stream.nil? and (resume.nil? or resume == 'enrich') and not skips.include?('enrich')),
          :staging_stream_enrich => ((not enriched_stream.nil? and resume.nil?) and not skips.include?('staging_stream_enrich')),
          :shred => ((resume.nil? or [ 'enrich', 'shred' ].include?(resume)) and
            not skips.include?('shred')),
          :es => ((resume.nil? or [ 'enrich', 'shred', 'elasticsearch' ].include?(resume)) and
            not skips.include?('elasticsearch')),
          :archive_raw => (enriched_stream.nil? and (resume.nil? or [ 'enrich', 'shred', 'elasticsearch', 'archive_raw' ].include?(resume)) and
            not skips.include?('archive_raw')),
          :rdb_load => ((resume.nil? or [ 'enrich', 'shred', 'elasticsearch', 'archive_raw', 'rdb_load' ].include?(resume)) and
            not skips.include?('rdb_load')),
          :consistency_check => ((resume.nil? or [ 'enrich', 'shred', 'elasticsearch', 'archive_raw', 'rdb_load', 'consistency_check' ].include?(resume)) and
            not skips.include?('consistency_check')),
          :load_manifest_check => ((resume.nil? or [ 'enrich', 'shred', 'elasticsearch', 'archive_raw', 'rdb_load'  ].include?(resume)) and
            not skips.include?('load_manifest_check')),
          :analyze => ((resume.nil? or [ 'enrich', 'shred', 'elasticsearch', 'archive_raw', 'rdb_load', 'consistency_check', 'load_manifest_check', 'analyze' ].include?(resume)) and
            not skips.include?('analyze')),
          :archive_enriched => ((resume.nil? or
            [ 'enrich', 'shred', 'elasticsearch', 'archive_raw', 'rdb_load', 'consistency_check', 'analyze', 'archive_enriched' ].include?(resume)) and
            not skips.include?('archive_enriched')),
          :archive_shredded => (not skips.include?('archive_shredded'))
        }
      end


      Contract HashOf[Symbol, Bool], ArrayOf[String] => RdbLoaderSteps
      def self.get_rdbloader_steps(steps, inclusions)
        s = {
          :skip => [],
          :include => []
        }

        if not steps[:analyze]
          s[:skip] << "analyze"
        end

        if not steps[:consistency_check]
          s[:skip] << "consistency_check"
        end

        if not steps[:load_manifest_check]
          s[:skip] << "load_manifest_check"
        end

        if inclusions.include?("vacuum")
          s[:include] << "vacuum"
        end

        s
      end

      # Initialize the class.
      Contract ArgsHash, ConfigHash, ArrayOf[String], String, ArrayOf[JsonFileHash] => Runner
      def initialize(args, config, enrichments_array, resolver, targets_array)

        # Let's set our logging level immediately
        Monitoring::Logging::set_level config[:monitoring][:logging][:level]

        @args = args
        @config = config
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
        steps = Runner.get_steps(@args[:skip], @args[:resume_from], @config[:aws][:s3][:buckets][:enriched][:stream])

        archive_enriched = if not steps[:archive_enriched]
          'skip'
        elsif steps[:enrich] || steps[:staging_stream_enrich]
          'pipeline'
        else
          'recover'
        end

        archive_shredded = if not steps[:archive_shredded]
          'skip'
        elsif steps[:shred]
          'pipeline'
        else
          'recover'
        end

        lock = get_lock(@args[:lock], @args[:consul])
        if not lock.nil? and not @args[:ignore_lock_on_start]
          lock.try_lock
        end

        # Keep relaunching the job until it succeeds or fails for a reason other than a bootstrap failure
        tries_left = @config[:aws][:emr][:bootstrap_failure_tries]
        rdbloader_steps = Runner.get_rdbloader_steps(steps, @args[:include])
        while true
          begin
            tries_left -= 1
            job = EmrJob.new(@args[:debug], steps[:staging], steps[:enrich], steps[:staging_stream_enrich], steps[:shred], steps[:es],
              steps[:archive_raw], steps[:rdb_load], archive_enriched, archive_shredded, @config,
              @enrichments_array, @resolver_config, @targets, rdbloader_steps, @args[:use_persistent_jobflow])
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
          rescue DirectoryNotEmptyError, NoDataToProcessError => e
            # unlock on no-op
            if not lock.nil?
              lock.unlock
            end
            raise e
          end
        end

        if not lock.nil?
          lock.unlock
        end

        logger.info "Completed successfully"
        nil
      end

      def get_lock(path, consul)
        if not path.nil?
          if not consul.nil?
            Lock::ConsulLock.new(consul, path)
          else
            Lock::FileLock.new(path)
          end
        else
          nil
        end
      end

      # Adds trailing slashes to all non-nil bucket names in the hash
      def self.add_trailing_slashes(bucketData)
        if bucketData.class == ''.class
          bucketData[-1].chr != '/' ? bucketData << '/' : bucketData
        elsif bucketData.class == {}.class
          bucketData.each {|k,v| add_trailing_slashes(v)}
        elsif bucketData.class == [].class
          bucketData.each {|b| add_trailing_slashes(b)}
        end
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

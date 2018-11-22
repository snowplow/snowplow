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

require 'optparse'
require 'yaml'
require 'json'
require 'contracts'
require 'erb'

# To have a command's description
class OptionParser
  attr_accessor :description
end

module Snowplow
  module EmrEtlRunner
    module Cli

      include Contracts

      # Supported options
      COLLECTOR_FORMAT_REGEX = /^(?:cloudfront|clj-tomcat|thrift|(?:json\/.+\/.+)|(?:tsv\/.+\/.+)|(?:ndjson\/.+\/.+))$/
      RESUMABLES = Set.new(%w(enrich shred elasticsearch archive_raw rdb_load analyze archive_enriched archive_shredded staging_stream_enrich))
      SKIPPABLES = Set.new(%w(staging enrich shred elasticsearch archive_raw rdb_load consistency_check analyze load_manifest_check archive_enriched archive_shredded staging_stream_enrich))
      INCLUDES = Set.new(%w(vacuum))

      # Get our arguments, configuration,
      # enrichments and Iglu resolver.
      #
      # Source from parse_args (i.e. the CLI)
      # unless both are provided as arguments
      # to this function
      #
      # Parameters:
      # +config_file+:: the YAML file containing our
      #                 configuration options
      #
      # Returns a Hash containing our runtime
      # arguments and our configuration.
      Contract None => ArgsConfigEnrichmentsResolverTuple
      def self.get_args_config_enrichments_resolver

        # Defaults
        options = {
          :debug => false,
          :skip => [],
          :include => [],
          :ignore_lock_on_start => false,
          :use_persistent_jobflow => false,
        }

        commands = {
          'run' => OptionParser.new do |opts|
            opts.banner = 'Usage: run [options]'
            opts.description = 'Run the Snowplow pipeline on EMR'
            opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config_file] = config }
            opts.on('-n', '--enrichments ENRICHMENTS', 'enrichments directory') { |config| options[:enrichments_directory] = config }
            opts.on('-r', '--resolver RESOLVER', 'Iglu resolver file') { |config| options[:resolver_file] = config }
            opts.on('-t', '--targets TARGETS', 'targets directory') { |config| options[:targets_directory] = config }
            opts.on('-d', '--debug', 'enable EMR Job Flow debugging') { |config| options[:debug] = true }
            opts.on('-f', "--resume-from {#{RESUMABLES.to_a.join(',')}}", 'resume from the specified step') { |config| options[:resume_from] = config }
            opts.on('-x', "--skip {#{SKIPPABLES.to_a.join(',')}}", Array, 'skip the specified step(s)') { |config| options[:skip] = config }
            opts.on('-i', "--include {#{INCLUDES.to_a.join(',')}}", Array, 'include additional step(s)') { |config| options[:include] = config }
            opts.on('-l', '--lock PATH', 'where to store the lock') { |config| options[:lock] = config }
            opts.on('--ignore-lock-on-start', 'ignore the lock if it is set when starting') { |config| options[:ignore_lock_on_start] = true }
            opts.on('--consul ADDRESS', 'address to the Consul server') { |config| options[:consul] = config }
            opts.on('--use-persistent-jobflow', 'discovers and uses a persistent cluster for steps') { |config| options[:use_persistent_jobflow] = true }
          end,
          'generate emr-config' => OptionParser.new do |opts|
            opts.banner = 'Usage: generate emr-config [options]'
            opts.description = 'Generate a Dataflow Runner EMR config which can be used with dataflow-runner up'
            opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config_file] = config }
            opts.on('-r', '--resolver RESOLVER', 'Iglu resolver file') { |config| options[:resolver_file] = config }
            opts.on('-f', '--filename FILENAME', 'the name of the file to generate') { |config| options[:filename] = config }
            opts.on('-s', '--schemaver VERSION', 'the version of the Avro schema to use') { |config| options[:schemaver] = config }
          end,
          'generate emr-playbook' => OptionParser.new do |opts|
            opts.banner = 'Usage: generate emr-playbook [options]'
            opts.description = 'Generate a Dataflow Runner Playbook config which can be used with dataflow-runner run'
            opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config_file] = config }
            opts.on('-n', '--enrichments ENRICHMENTS', 'enrichments directory') { |config| options[:enrichments_directory] = config }
            opts.on('-r', '--resolver RESOLVER', 'Iglu resolver file') { |config| options[:resolver_file] = config }
            opts.on('-d', '--debug', 'enable EMR Job Flow debugging') { |config| options[:debug] = true }
            opts.on('-f', "--resume-from {#{RESUMABLES.to_a.join(',')}}", 'resume from the specified step') { |config| options[:resume_from] = config }
            opts.on('-x', "--skip {#{SKIPPABLES.to_a.join(',')}}", Array, 'skip the specified step(s)') { |config| options[:skip] = config }
            opts.on('-i', "--include {#{INCLUDES.to_a.join(',')}}", Array, 'include additional step(s)') { |config| options[:include] = config }
            opts.on('-o', '--filename FILENAME', 'the name of the file to generate') { |config| options[:filename] = config }
            opts.on('-s', '--schemaver VERSION', 'the version of the Avro schema to use') { |config| options[:schemaver] = config }
          end,
          'generate all' => OptionParser.new do |opts|
            opts.banner = 'Usage: generate all [options]'
            opts.description = 'Generate both a Dataflow Runner EMR (as emr-config.json) and Playbook (as emr-playbook.json) configs'
            opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config_file] = config }
            opts.on('-n', '--enrichments ENRICHMENTS', 'enrichments directory') { |config| options[:enrichments_directory] = config }
            opts.on('-r', '--resolver RESOLVER', 'Iglu resolver file') { |config| options[:resolver_file] = config }
            opts.on('-d', '--debug', 'enable EMR Job Flow debugging') { |config| options[:debug] = true }
            opts.on('-f', "--resume-from {#{RESUMABLES.to_a.join(',')}}", 'resume from the specified step') { |config| options[:resume_from] = config }
          end,
          'lint resolver' => OptionParser.new do |opts|
            opts.banner = 'Usage: lint resolver [options]'
            opts.description = 'Lint an Iglu resolver config to check if it is valid with respect to its schema'
            opts.on('-r', '--resolver RESOLVER', 'Iglu resolver file') { |config| options[:resolver_file] = config }
          end,
          'lint enrichments' => OptionParser.new do |opts|
            opts.banner = 'Usage: lint enrichments [options]'
            opts.description = 'Lint enrichments to check if they are valid with respect to their schemas'
            opts.on('-r', '--resolver RESOLVER', 'Iglu resolver file') { |config| options[:resolver_file] = config }
            opts.on('-n', '--enrichments ENRICHMENTS', 'enrichments directory') { |config| options[:enrichments_directory] = config }
          end,
          'lint all' => OptionParser.new do |opts|
            opts.banner = 'Usage: lint all [options]'
            opts.description = 'Lint both Iglu resolver config and enrichments to check if they are valid with respect to their schemas'
            opts.on('-r', '--resolver RESOLVER', 'Iglu resolver file') { |config| options[:resolver_file] = config }
            opts.on('-n', '--enrichments ENRICHMENTS', 'enrichments directory') { |config| options[:enrichments_directory] = config }
          end
        }

        global = OptionParser.new do |opts|
          opts.banner = 'Usage %s [options] [command [options]]' % NAME
          opts.separator ''
          opts.separator 'Available commands are:'
          commands.each_pair do |c, opt|
            desc = opt.description
            opts.separator "#{c}: #{desc}"
          end
          opts.separator ''
          opts.separator 'Global options are:'
          opts.on('-v', '--version', 'Show version') do
            puts '%s %s' % [NAME, VERSION]
            exit
          end
        end

        # Run OptionParser's structural validation
        begin
          global.order!
          cmd_name = ARGV.shift
          if cmd_name
            cmd = commands[cmd_name]
            if cmd
              cmd.order!
            else
              sub_cmd_name = ARGV.shift
              if sub_cmd_name
                cmd_name += " #{sub_cmd_name}"
                cmd = commands[cmd_name]
                if cmd
                  cmd.order!
                else
                  exit1("Invalid command: #{cmd_name}", global)
                end
              else
                exit1("Invalid command: #{cmd_name}", global)
              end
            end
          else
            exit1('Empty command!', global)
          end
          [cmd_name] + process_options(options, cmd, cmd_name)
        rescue OptionParser::InvalidOption, OptionParser::MissingArgument
          raise ConfigError, "#{$!.to_s}\n#{cmd}"
        end
      end

      def self.exit1(msg, help)
        puts msg
        puts help
        exit 1
      end

      def self.process_options(options, optparse, cmd_name)
        args = {
          :debug => options[:debug],
          :skip => options[:skip],
          :resume_from => options[:resume_from],
          :include => options[:include],
          :lock => options[:lock],
          :ignore_lock_on_start => options[:ignore_lock_on_start],
          :consul => options[:consul],
          :use_persistent_jobflow => options[:use_persistent_jobflow],
        }

        summary = optparse.to_s
        config = load_config(options[:config_file], summary,
          (not cmd_name.include?('lint')))
        enrichments = load_enrichments(options[:enrichments_directory], summary,
          [ 'lint all', 'lint enrichments' ].include?(cmd_name))
        resolver = load_resolver(options[:resolver_file], summary)
        targets = load_targets(options[:targets_directory])

        [args, config.nil? ? nil : validate_and_coalesce(args, config), enrichments, resolver, targets]
      end

      # Validate our args, load our config YAML, check config and args don't conflict
      Contract Maybe[String], String, Bool => Maybe[ConfigHash]
      def self.load_config(config_file, summary, is_required=true)
        return nil if not is_required

        # Check we have a config file argument and it exists
        if config_file.nil?
          raise ConfigError, "Missing option: config\n#{summary}"
        end

        # A single hyphen indicates that the config should be read from stdin
        if config_file == '-'
          config = $stdin.readlines.join
        elsif File.file?(config_file)
          config = File.new(config_file).read
        else
          raise ConfigError, "Configuration file '#{config_file}' does not exist, or is not a file\n#{summary}"
        end

        erb_config = ERB.new(config).result(binding)

        recursive_symbolize_keys(YAML.load(erb_config))
      end

      # Load the enrichments directory into an array
      Contract Maybe[String], String, Bool => ArrayOf[String]
      def self.load_enrichments(enrichments_dir, summary, is_required=true)
        return [] if enrichments_dir.nil? and not is_required
        if enrichments_dir.nil? and is_required
          raise ConfigError, "Missing option: enrichments\n#{summary}"
        end

        # Check the enrichments directory exists and is a directory
        unless Dir.exist?(enrichments_dir)
          raise ConfigError, "Enrichments directory '#{enrichments_dir}' does not exist, or is not a directory\n#{summary}"
        end

        json_glob =
          (enrichments_dir[-1].chr != '/' ? enrichments_dir << '/' : enrichments_dir) + '*.json'

        Dir.glob(json_glob).map { |f|
          File.read(f)
        }
      end

      # Validate our args, load our Iglu resolver
      Contract Maybe[String], String, Bool => String
      def self.load_resolver(resolver_file, summary)
        # Check we have a resolver file argument and it exists
        if resolver_file.nil?
          raise ConfigError, "Missing option: resolver\n#{summary}"
        end

        unless File.file?(resolver_file)
          raise ConfigError, "Iglu resolver file '#{resolver_file}' does not exist, or is not a file\n#{summary}"
        end

        File.read(resolver_file)
      end

      # Load configuration from JSONs
      Contract Maybe[String] => ArrayOf[JsonFileHash]
      def self.load_targets(targets_path)
        ids = []
        targets = if targets_path.nil?
          []
        else
          Dir.entries(targets_path).select do |f|
            f.end_with?('.json')
          end.map do |f|
            json = JSON.parse(File.read(targets_path + '/' + f), {:symbolize_names => true})
            id = json.dig(:data, :id)
            unless id.nil?
              ids.push(id)
            end
            {:file => f, :json => json}
          end
        end
        duplicate_ids = ids.select { |id| ids.count(id) > 1 }.uniq
        unless duplicate_ids.empty?
          raise ConfigError, "Duplicate storage target ids: #{duplicate_ids}"
        end
        targets
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

      # Validate our arguments against the configuration Hash
      # Make updates to the configuration Hash based on the
      # arguments
      Contract ArgsHash, ConfigHash => ConfigHash
      def self.validate_and_coalesce(args, config)

        unless args[:resume_from].nil?
          unless RESUMABLES.include?(args[:resume_from])
            raise ConfigError, "Invalid option: resume-from can be #{RESUMABLES.to_a.join(', ')} not '#{args[:resume_from]}'"
          end
        end

        args[:skip].each { |opt|
          unless SKIPPABLES.include?(opt)
            raise ConfigError, "Invalid option: skip can be #{SKIPPABLES.to_a.join(', ')} not '#{opt}'"
          end
        }

        if not args[:resume_from].nil? and not args[:skip].empty?
          raise ConfigError, 'resume-from and skip are mutually exclusive'
        end

        if args[:resume_from] == "staging_stream_enrich" && config.dig(:aws, :s3, :buckets, :enriched, :stream).nil?
          raise ConfigError, 'staging_stream_enrich is invalid step to resume from without aws.s3.buckets.enriched.stream settings'
        end
        unless config.dig(:aws, :s3, :buckets, :enriched, :stream).nil?
          if args[:resume_from] == "enrich"
            raise ConfigError, 'cannot resume from enrich in stream enrich mode'
          end
          if args[:skip].include?('staging') || args[:skip].include?('enrich')
            raise ConfigError, 'cannot skip staging nor enrich in stream enrich mode. Either skip staging_stream_enrich or resume from shred'
          end
          if args[:skip].include?('archive_raw') || args[:resume_from] == "archive_raw"
              raise ConfigError, 'cannot skip nor resume from archive_raw in stream enrich mode'
          end
        end

        args[:include].each { |opt|
          unless INCLUDES.include?(opt)
            raise ConfigError, "Invalid option: include can be #{INCLUDES.to_a.join(', ')} not '#{opt}'"
          end
        }

        collector_format = config.dig(:collectors, :format)

        # Validate the collector format
        unless collector_format.nil? || collector_format =~ COLLECTOR_FORMAT_REGEX
          raise ConfigError, "collector_format '%s' not supported" % collector_format
        end

        if collector_format == 'thrift' and config[:aws][:emr][:ami_version].start_with?('2')
          raise ConfigError, "Cannot process Thrift events with AMI version 2.x.x"
        end

        # Add trailing slashes if needed to the non-nil buckets
        config[:aws][:s3][:buckets] = add_trailing_slashes(config[:aws][:s3][:buckets])

        config
      end

    private

      # Convert all keys in arbitrary hash into symbols
      # Taken from http://stackoverflow.com/a/10721936/255627
      def self.recursive_symbolize_keys(h)
        case h
        when Hash
          Hash[
            h.map do |k, v|
              [ k.respond_to?(:to_sym) ? k.to_sym : k, recursive_symbolize_keys(v) ]
            end
          ]
        when Enumerable
          h.map { |v| recursive_symbolize_keys(v) }
        else
          h
        end
      end

    end
  end
end

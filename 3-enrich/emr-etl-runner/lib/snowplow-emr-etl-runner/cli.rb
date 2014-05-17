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

require 'optparse'
require 'yaml'

require 'contracts'
include Contracts

module Snowplow
  module EmrEtlRunner
    module Cli

      # Supported options
      @@collector_formats = Set.new(%w(cloudfront clj-tomcat))
      @@skip_options = Set.new(%w(staging emr archive))

      # Get our arguments and configuration.
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
      Contract Maybe[String], Maybe[Bool], Maybe[String], Maybe[String], Maybe[ArrayOf[String]], Maybe[String] => ArgsConfigHash
      def self.get_args_config(config_file=nil, debug=nil, start=nil, _end=nil, skip=nil, process_bucket=nil)

        # Try the CLI if we don't have args passed in
        if config_file.nil? || debug.nil? || start.nil? || _end.nil? || skip.nil? || process.nil? then
          options = parse_args

          config_file = options[:config_file]

          debug = options[:debug]
          start = options[:start]
          _end = options[:end]
          skip = options[:skip]
          process_bucket = options[:process_bucket]
        end

        config = validate_and_load(config_file, start, _end, skip, process_bucket)

        # Return our args & config
        {
          :args => {
            :debug => debug,
            :start => start,
            :end => _end,
            :skip => skip,
            :process_bucket => process_bucket
          },
          :config => config
        }
      end

      private

      # Parse the command-line arguments
      # Returns: the hash of parsed options
      #
      # Returns a hash of parsed arguments
      Contract None => ParsedArgsHash
      def self.parse_args

        # Defaults
        options = {
          :skip => [],
          :debug = false
        }

        optparse = OptionParser.new do |opts|

          opts.banner = "Usage: %s [options]" % NAME
          opts.separator ""
          opts.separator "Specific options:"

          opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config] = config }
          opts.on('-d', '--debug', 'enable EMR Job Flow debugging') { |config| options[:debug] = true }
          opts.on('-s', '--start YYYY-MM-DD', 'optional start date *') { |config| options[:start] = config }
          opts.on('-e', '--end YYYY-MM-DD', 'optional end date *') { |config| options[:end] = config }
          opts.on('-s', '--skip staging,emr,archive', Array, 'skip work step(s)') { |config| options[:skip] = config }
          opts.on('-b', '--process-bucket BUCKET', 'run emr only on specified bucket. Implies --skip staging,archive') { |config| 
            options[:processbucket] = config
            options[:skip] = %w(staging archive)
          }

          opts.separator ""
          opts.separator "* filters the raw event logs processed by EmrEtlRunner by their timestamp. Only"
          opts.separator "  supported with 'cloudfront' collector format currently."

          opts.separator ""
          opts.separator "Common options:"

          opts.on_tail('-h', '--help', 'Show this message') { puts opts; exit }
          opts.on_tail('-v', "--version", "Show version") do
            puts "%s %s" % [NAME, VERSION]
            exit
          end
        end

        # Run OptionParser's structural validation
        begin
          optparse.parse!
        rescue OptionParser::InvalidOption, OptionParser::MissingArgument
          raise ConfigError, "#{$!.to_s}\n#{optparse}"
        end

        options
      end

      Contract String, Maybe[String], Maybe[String], Maybe[ArrayOf[String]], Maybe[String] => ConfigHash
      def self.validate_and_load(config_file, start, _end, skip, process_bucket)

        # Check we have a config file argument and it exists
        if config_file.nil?
          raise ConfigError, "Missing option: config\n#{optparse}"
        end
        unless File.file?(config_file)
          raise ConfigError, "Configuration file '#{config_file}' does not exist, or is not a file."
        end

        # Check our skip argument
        skip.each { |opt|
          unless @@skip_options.include?(opt)
            raise ConfigError, "Invalid option: skip can be 'staging', 'emr' or 'archive', not '#{opt}'"
          end
        }

        # Check that start is before end, if both set
        if !start.nil? and !_end.nil?
          if start > _end
            raise ConfigError, "Invalid options: end date '#{_end}' is before start date '#{start}'"
          end
        end

        config = YAML.load_file(config_file)

        # Validate the collector format
        unless @@collector_formats.include?(config[:etl][:collector_format]) 
          raise ConfigError, "collector_format '%s' not supported" % config[:etl][:collector_format]
        end

        # Currently we only support start/end times for the CloudFront collector format. See #120 for details
        unless config[:etl][:collector_format] == 'cloudfront' or (start.nil? and _end.nil?)
          raise ConfigError, "--start and --end date arguments are only supported if collector_format is 'cloudfront'"
        end

        unless process_bucket.nil?
          config[:s3][:buckets][:processing] = process_bucket
        end

        # Add trailing slashes if needed to the non-nil buckets
        config[:s3][:buckets].reject{|k,v| v.nil?}.update(config[:s3][:buckets]){|k,v| Sluice::Storage::trail_slash(v)}

        config
      end

    end
  end
end

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
      Contract None => ArgsConfigTuple
      def self.get_args_config
        
        # Defaults
        options = {
          :skip => [],
          :debug => false
        }

        optparse = OptionParser.new do |opts|

          opts.banner = "Usage: %s [options]" % NAME
          opts.separator ""
          opts.separator "Specific options:"

          opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config_file] = config }
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

        args = {
          :debug          => options[:debug],
          :start          => options[:start],
          :end            => options[:end],
          :skip           => options[:skip],
          :process_bucket => options[:process_bucket]
        }
        config = load_file(options[:config_file], optparse.to_s)

        [args, config]
      end

    private

      # Validate our args, load our config YAML, check config and args don't conflict
      Contract Maybe[String], String => ConfigHash
      def self.load_file(config_file, optparse)

        # Check we have a config file argument and it exists
        if config_file.nil?
          raise ConfigError, "Missing option: config\n#{optparse}"
        end

        unless File.file?(config_file)
          raise ConfigError, "Configuration file '#{config_file}' does not exist, or is not a file\n#{optparse}"
        end

        YAML.load_file(config_file)
      end

    end
  end
end

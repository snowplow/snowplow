# Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2013 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'optparse'
require 'date'
require 'yaml'
require 'sluice'

# Config module to hold functions related to CLI argument parsing
# and config file reading to support storage loading.
module SnowPlow
  module StorageLoader
    module Config

      # TODO: would be nice to move this to using Kwalify
      # TODO: would be nice to support JSON as well as YAML

      @@storage_targets = Set.new(%w(redshift infobright))

      # Return the configuration loaded from the supplied YAML file, plus
      # the additional constants above.
      def get_config()

        options = Config.parse_args()
        config = YAML.load_file(options[:config])

        # Add in our skip setting
        config[:skip] = options[:skip]

        # Add trailing slashes if needed to the buckets and download folder
        config[:s3][:buckets].update(config[:s3][:buckets]){|k,v| Sluice::Storage::trail_slash(v)}
        config[:download][:folder] = Sluice::Storage::trail_slash(config[:download][:folder])

        # Check we recognise the storage target 
        unless @@storage_targets.include?(config[:storage][:type]) 
          raise ConfigError, "Storage type '#{config[:storage][:type]}' not supported"
        end
            
        # If Infobright is the target, check that the download folder exists and is empty
        if (config[:storage][:type] == 'infobright')
          # Check that the download folder exists...
          unless File.directory?(config[:download][:folder])
            raise ConfigError, "Download folder '#{config[:download][:folder]}' not found"
          end
        
          # ...and it is empty
          unless config[:skip].include?("download")
            if !(Dir.entries(config[:download][:folder]) - %w{ . .. }).empty?
              raise ConfigError, "Download folder '#{config[:download][:folder]}' is not empty"
            end
          end
        end

        config
      end  
      module_function :get_config

      private

      # Parse the command-line arguments
      # Returns: the hash of parsed options
      def parse_args()

        # Handle command-line arguments
        options = {}
        options[:skip] = []
        optparse = OptionParser.new do |opts|

          opts.banner = "Usage: %s [options]" % NAME
          opts.separator ""
          opts.separator "Specific options:"

          opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config] = config }
          opts.on('-s', '--skip download|delete,load,archive', Array, 'skip work step(s)') { |config| options[:skip] = config }

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

        # Check our skip argument
        options[:skip].each { |opt|
          unless %w(download delete load archive).include?(opt)
            raise ConfigError, "Invalid option: skip can be 'download', 'delete', 'load' or 'archive', not '#{opt}'"
          end
        }

        # Check we have a config file argument
        if options[:config].nil?
          raise ConfigError, "Missing option: config\n#{optparse}"
        end

        # Check the config file exists
        unless File.file?(options[:config])
          raise ConfigError, "Configuration file '#{options[:config]}' does not exist, or is not a file."
        end

        options
      end
      module_function :parse_args

    end
  end
end
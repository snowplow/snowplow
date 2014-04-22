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
# and config file reading to support the daily ETL job.
module SnowPlow
  module EmrEtlRunner
    module Config

      @@collector_formats = Set.new(%w(cloudfront clj-tomcat thrift-raw))
      @@storage_formats = Set.new(%w(hive redshift mysql-infobright))

      # TODO: would be nice to move this to using Kwalify
      # TODO: would be nice to support JSON as well as YAML

      # Return the configuration loaded from the supplied YAML file, plus
      # the additional constants above.
      def get_config()

        options = Config.parse_args()
        config = YAML.load_file(options[:config])

        # Add in the start and end dates, and our skip and debug settings
        config[:start] = options[:start]
        config[:end] = options[:end]
        config[:skip] = options[:skip]
        config[:debug] = options[:debug]

        # Generate our run ID: based on the time now
        config[:run_id] = Time.new.strftime("%Y-%m-%d-%H-%M-%S")

        unless options[:processbucket].nil?
          config[:s3][:buckets][:processing] = options[:processbucket]
        end

        # Add trailing slashes if needed to the non-nil buckets
        config[:s3][:buckets].reject{|k,v| v.nil?}.update(config[:s3][:buckets]){|k,v| Sluice::Storage::trail_slash(v)}

        # TODO: can we get this functionality for free with Fog?
        if config[:s3][:region] == "us-east-1"
          config[:s3][:endpoint] = "s3.amazonaws.com"
        else
          config[:s3][:endpoint] = "s3-%s.amazonaws.com" % config[:s3][:region]
        end

        # We have to rename some config fields for Elasticity - and make a manual adjustment
        config[:emr][:jobflow][:slave_instance_type] = config[:emr][:jobflow][:core_instance_type]
        config[:emr][:jobflow][:instance_count] = config[:emr][:jobflow][:core_instance_count] + 1 # +1 for the master instance
        config[:emr][:jobflow].delete_if {|k, _| k.to_s.start_with?("core_") }

        # Validate the collector format
        unless @@collector_formats.include?(config[:etl][:collector_format])
          raise ConfigError, "collector_format '%s' not supported" % config[:etl][:collector_format]
        end

        # Currently we only support start/end times for the CloudFront collector format. See #120 for details
        unless config[:etl][:collector_format] == 'cloudfront' or (config[:start].nil? and config[:end].nil?)
          raise ConfigError, "--start and --end date arguments are only supported if collector_format is 'cloudfront'"
        end

        # Construct path to our assets
        if config[:s3][:buckets][:assets] == "s3://snowplow-hosted-assets/"
          asset_host = "http://snowplow-hosted-assets.s3.amazonaws.com/" # Use the public S3 URL
        else
          asset_host = config[:s3][:buckets][:assets]
        end
        config[:maxmind_asset] = "%sthird-party/maxmind/GeoLiteCity.dat" % asset_host

        # Construct our path to S3DistCp
        config[:s3distcp_asset] = "/home/hadoop/lib/emr-s3distcp-1.0.jar"

        # Construct path to our Hadoop ETL
        config[:hadoop_asset] = "%s3-enrich/hadoop-etl/snowplow-hadoop-etl-%s.jar" % [
                                  config[:s3][:buckets][:assets],
                                  config[:etl][:hadoop_etl_version]
                                ]

        # Should we continue on unexpected error or not?
        continue_on = case config[:etl][:continue_on_unexpected_error]
                        when true
                          '1'
                        when false
                          '0'
                        else
                          raise ConfigError, "continue_on_unexpected_error '%s' not supported (only 'true' or 'false')" % config[:etl][:continue_on_unexpected_error]
                        end
        config[:etl][:continue_on_unexpected_error] = continue_on # Heinous mutability

        # Now let's handle the enrichments.
        anon_octets = if config[:enrichments][:anon_ip][:enabled]
                        config[:enrichments][:anon_ip][:anon_octets].to_s
                      else
                        '0' # Anonymize 0 quartets == anonymization disabled
                      end
        config[:enrichments][:anon_ip_octets] = anon_octets

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
        options[:debug] = false
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

        # Check our skip argument
        options[:skip].each { |opt|
          unless %w(staging emr archive).include?(opt)
            raise ConfigError, "Invalid option: skip can be 'staging', 'emr' or 'archive', not '#{opt}'"
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

        # Finally check that start is before end, if both set
        if !options[:start].nil? and !options[:end].nil?
          if options[:start] > options[:end]
            raise ConfigError, "Invalid options: end date '#{options[:end]}' is before start date '#{options[:start]}'"
          end
        end

        options
      end
      module_function :parse_args

    end
  end
end

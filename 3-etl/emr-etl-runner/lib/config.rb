# Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'optparse'
require 'date'
require 'yaml'

# Config module to hold functions related to CLI argument parsing
# and config file reading to support the daily ETL job.
module Config

  # What are we called?
  SCRIPT_NAME = SnowPlow::EmrEtlRunner::SCRIPT_NAME

  # Where to find our HiveQL queries
  QUERY_PATH = File.join("..", "..", "hive-etl", "hiveql")
  DAILY_QUERY_FILE = "daily-etl.q"
  DATESPAN_QUERY_FILE = "datespan-etl.q"

  # Where to find the Hive Serde used by our queries
  SERDE_PATH = File.join("..", "..", "hive-etl", "snowplow-log-deserializers", 'upload')
  SERDE_FILE = "snowplow-log-deserializers-%s.jar" % SnowPlow::EmrEtlRunner::HIVE_SERDE_VERSION

  # Return the configuration loaded from the supplied YAML file, plus
  # the additional constants above.
  def Config.get_config()

    options = Config.parse_args()
    config = YAML.load_file(options[:config])

    # Add in the start and end dates
    config[:start] = options[:start]
    config[:end] = options[:end]

    # Add trailing slashes if needed to the buckets
    trail = lambda {|str| return str[-1].chr != '/' ? str << '/' : str}
    config[:buckets].update(config[:s3][:buckets]){|k,v| trail.call(v)}

    config[:hadoop_version] = SnowPlow::EmrEtlRunner::HADOOP_VERSION
    config[:s3_location] = config[:s3][:location]
    config[:emr_placement] = config[:emr][:placement]
    config[:ec2_key_name] = config[:emr][:ec2_key_name]

    config[:daily_query_file] = DAILY_QUERY_FILE
    config[:daily_query_path] = File.join(File.dirname(__FILE__), QUERY_PATH, DAILY_QUERY_FILE)
    config[:datespan_query_file] = DATESPAN_QUERY_FILE
    config[:datespan_query_path] = File.join(File.dirname(__FILE__), QUERY_PATH, DATESPAN_QUERY_FILE)
    config[:serde_path] = File.join(File.dirname(__FILE__), SERDE_PATH, SERDE_FILE)
    config[:serde_file] = SERDE_FILE

    config
  end

  # Parse the command-line arguments
  # Returns: the hash of parsed options
  def Config.parse_args()

    # Handle command-line arguments
    options = {}
    optparse = OptionParser.new do |opts|

      opts.banner = "Usage: %s [options]" % SCRIPT_NAME
      opts.separator ""
      opts.separator "Specific options:"

      opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config] = config }
      opts.on('-s', '--start YYYY-MM-DD', 'optional start date (defaults to yesterday)') { |config| options[:start] = config }
      opts.on('-e', '--end YYYY-MM-DD', 'optional end date (defaults to yesterday)') { |config| options[:end] = config }

      opts.separator ""
      opts.separator "Common options:"

      opts.on_tail('-h', '--help', 'Show this message') { puts opts; exit }
      opts.on_tail('-v', "--version", "Show version") do
        puts "%s %s" % [SCRIPT_NAME, SnowPlow::Etl::VERSION] #.join('.')
        exit
      end
    end

    # Set defaults
    yesterday = (Date.today - 1).strftime('%Y-%m-%d') # Yesterday's date
    options[:start] ||= yesterday
    options[:end]   ||= yesterday

    # Check the mandatory arguments
    begin
      optparse.parse!
      mandatory = [:config, :start, :end]
      missing = mandatory.select{ |param| options[param].nil? }
      if not missing.empty?
        raise ConfigError, "Missing options: #{missing.join(', ')}\n#{optparse}"
      end
    rescue OptionParser::InvalidOption, OptionParser::MissingArgument
      raise ConfigError, "#{$!.to_s}\n#{optparse}"
    end

    # Finally check that start is before end
    if options[:start] > options[:end]
      raise ConfigError, "Invalid options: end date #{options[:end]} is before start date #{options[:start]}"
    end

    options
  end

  class ConfigError < ArgumentError
  end

end
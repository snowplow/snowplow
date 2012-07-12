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

# Author::    Alex Dean (mailto:alex@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'optparse'
require 'date'
require 'yaml'

# Config module to hold functions related to CLI argument parsing
# and config file reading to support the daily ETL job.
module Config

  # What are we called?
  SCRIPT_NAME = "snowplow-etl"

  # Where to find our HiveQL queries
  QUERY_PATH = File.join("..", "hiveql")
  DAILY_QUERY_FILE = "daily-etl.q"
  DATESPAN_QUERY_FILE = "datespan-etl.q"

  # Where to find the Hive Serde used by our queries
  SERDE_PATH = File.join("..", "..", "snowplow-log-deserializers", 'upload')
  SERDE_FILE = "snowplow-log-deserializers-0.4.6.jar"

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
    config[:buckets].update(config[:buckets]){|k,v| trail.call(v)}

    config[:hive_version] = SnowPlow::Etl::HIVE_VERSION

    config[:daily_query_file] = DAILY_QUERY_FILE
    config[:daily_query_path] = File.join(File.dirname(__FILE__), QUERY_PATH, DAILY_QUERY_FILE)
    config[:datespan_query_file] = DATESPAN_QUERY_FILE
    config[:datespan_query_path] = File.join(File.dirname(__FILE__), QUERY_PATH, DATESPAN_QUERY_FILE)
    config[:serde_path] = File.join(File.dirname(__FILE__), SERDE_PATH, SERDE_FILE)
    config[:serde_file] = File.join(config[:buckets][:serde], SERDE_FILE)

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
        puts "Missing options: #{missing.join(', ')}"
        puts optparse
        exit -1
      end
    rescue OptionParser::InvalidOption, OptionParser::MissingArgument
      puts $!.to_s
      puts optparse
      exit -1
    end

    # Finally check that start is before end
    if options[:start] > options[:end]
      puts "Invalid options: end date %s is before start date %s" % [options[:end], options[:start]]
      puts optparse
      exit -1
    end

    options
  end

end

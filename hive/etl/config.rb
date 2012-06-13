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

  QUERY_FILE = "snowplow-etl.q"
  SERDE_FILE = "snowplow-log-deserializers-0.4.4.jar"
  HIVE_VERSION = "0.7"

  # Return the configuration loaded from the supplied YAML file, plus
  # the additional constants above.
  def Config.get_config()

    options = Config.parse_args()
    config = YAML.load_file(options[:config])

    config[:date] = (Date.today - 1).strftime('%Y-%m-%d') # Yesterday's date
    config[:query_file][:local] = File.join(File.dirname(__FILE__), "hiveql", QUERY_FILE)
    config[:query_file][:remote] = File.join(config[:buckets][:query], QUERY_FILE)
    config[:serde_file] = File.join(config[:buckets][:jar], SERDE_FILE)
    config[:hive_version] = HIVE_VERSION

    config
  end

  # Parse the command-line arguments
  # Returns: the hash of parsed options
  def Config.parse_args()

    # Handle command-line arguments
    # TODO: add support for specifying a date range
    options = {}
    optparse = OptionParser.new do |opts|
      opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config] = config }
      opts.on('-h', '--help', 'display this screen') { puts opts; exit }
    end

    # Check the mandatory arguments
    # TODO: raise exception don't exit -1 on error
    begin
      optparse.parse!
      mandatory = [:config]
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

    options
  end

end

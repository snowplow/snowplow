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

  QUERY_SUBFOLDER = "hiveql"
  QUERY_FILE = "snowplow-etl.q"
  SERDE_FILE = "snowplow-log-deserializers-0.4.4.jar"

  # Return the configuration loaded from the supplied YAML file, plus
  # the additional constants above.
  def Config.get_config()

    options = Config.parse_args()
    config = YAML.load_file(options[:config])

    # Add trailing slashes if needed
    trail = lambda {|str| return str[-1].chr != '/' ? str << '/' : str}
    config[:buckets].update(config[:buckets]){|k,v| trail.call(v)}

    config[:date] = (Date.today - 1).strftime('%Y-%m-%d') # Yesterday's date
    config[:query_file_local] = File.join(File.dirname(__FILE__), "..", QUERY_SUBFOLDER, QUERY_FILE)
    config[:query_file_remote] = File.join(config[:buckets][:query], QUERY_FILE)
    config[:serde_file] = File.join(config[:buckets][:jar], SERDE_FILE)
    config[:hive_version] = SnowPlow::Etl::HIVE_VERSION

    config
  end

  # Parse the command-line arguments
  # Returns: the hash of parsed options
  def Config.parse_args()

    # Handle command-line arguments
    options = {}
    optparse = OptionParser.new do |opts|

      opts.banner = "Usage: snowplow-etl [options]"
      opts.separator ""
      opts.separator "Specific options:"

      opts.on('-c', '--config CONFIG', 'configuration file') { |config| options[:config] = config }
      # TODO: add support for specifying a date range

      opts.separator ""
      opts.separator "Common options:"

      opts.on_tail('-h', '--help', 'Show this message') { puts opts; exit }
      opts.on_tail("--version", "Show version") do
        puts SnowPlow::Etl::VERSION.join('.')
        exit
      end
    end

    # Check the mandatory arguments
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

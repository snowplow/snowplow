# Copyright (c) 2012 Orderly Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
#
# This Ruby script runs the daily ETL (extract, transform, load)
# process which transforms the raw CloudFront log data into
# SnowPlow-formatted Hive data tables, optimised for analysis.

# This is a module to wrap the command-line interface for the daily
# ETL job.
#
# Author::    Alex Dean (mailto:alex@keplarllp.com)
# Copyright:: Copyright (c) 2012 Orderly Ltd
# License::   Apache License Version 2.0

require 'optparse'
require 'date'
require 'yaml'

# Config module to hold functions related to CLI argument
# parsing and config file reading
module Config

  # Return the config from YAML file, plus
  # yesterday's date for the operation.
  def Config.get_config()

    # Now load the configuration
    options = Config.parse_args()
    config = YAML.load_file(options[:config])

    # And add yesterday's date to the config
    config[:date] = (Date.today - 1).strftime('%Y-%m-%d')

    config # Return the config
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

    options # Return the options
  end

end

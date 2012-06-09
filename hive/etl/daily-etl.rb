#!/usr/bin/env ruby

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
#
# This is a three-step process:
# 1. Extract the CloudFront log files to a temporary SnowPlow event data table (using the custom Deserializer)
# 2. Load the temporary event data into the final SnowPlow data table, partitioning by date and user
# 3. Archive the CloudFront log files by moving them into a separate bucket
#
# Note that each step is only actioned if the previous step succeeded without error.
#
# This Ruby script is dependent on:
# 1. AWS::S3 - see https://github.com/marcel/aws-s3/blob/master/INSTALL
# 2. Amazon Elastic MapReduce Ruby Client - see http://aws.amazon.com/developertools/2264
#
# Please make sure that both of these are installed before running this script.
#
# Author::    Alex Dean (mailto:alex@keplarllp.com)
# Copyright:: Copyright (c) 2012 Orderly Ltd
# License::   Apache License Version 2.0

require 'optparse'
require 'date'
require 'yaml'

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
    exit
  end
rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  puts $!.to_s
  puts optparse
  exit
end

# Now load the configuration
config = YAML.load_file(options[:config])

# Determine yesterday's date
yesterday = (Date.today - 1).strftime('%Y-%m-%d')

# Runs a daily ETL job for the specific day.
# Uses the Elastic MapReduce Command Line Tool.
# Parameters:
# +day+:: the day to run the ETL job for
# +buckets+:: the hash of bucket names to pass in to the Hive script
def run_etl(day, buckets)
  puts day
end

# Now run the job
run_etl(yesterday, config)

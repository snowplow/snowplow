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
# Copyright:: Copyright (c) 2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'spec_helper'

Cli = Snowplow::EmrEtlRunner::Cli

describe Cli do

  ConfigError = Snowplow::EmrEtlRunner::ConfigError

  it 'raises a ConfigError if the config file argument was nil' do
    expect {
      Cli.load_config(nil, "<<usage message>>")
    }.to raise_exception( ConfigError, "Missing option: config\n<<usage message>>" )
  end

  it 'raises a ConfigError if the config file argument could not be found' do
    expect {
      Cli.load_config("/no/such/file", "<<usage message>>")
    }.to raise_exception( ConfigError, "Configuration file '/no/such/file' does not exist, or is not a file\n<<usage message>>" )
  end

  it 'raises a ConfigError if the config file argument is not a file' do
    expect {
      Cli.load_config("/tmp", "<<usage message>>")
    }.to raise_exception( ConfigError, "Configuration file '/tmp' does not exist, or is not a file\n<<usage message>>" )
  end

  it 'can convert all keys in a hash to symbols' do

    raw = {
      "s3" => {
        "buckets" => {
          "raw" => {
            "in" => "s3n://snowplow-ice-logs-snplow/",
            "some_number" => 23
          }
        }
      },
      "emr" => {
        "bootstrap" => [
          "action1",
          "action2"],
        :already_symbol => "hello",
        "no_value" => nil,
        "empty_array" => []
      },
      "hash_of_arrays" => [
        {
          :name => "A database",
          :already_symbol => "localhost",
          :port => 1234
        }
      ]
    }

    Cli.recursive_symbolize_keys(raw).should == {
      :s3 => {
        :buckets => {
          :raw => {
            :in => "s3n://snowplow-ice-logs-snplow/",
            :some_number => 23
          }
        }
      },
      :emr => {
        :bootstrap => [
          "action1",
          "action2"
        ],
        :already_symbol => "hello",
        :no_value => nil,
        :empty_array => []
      },
      :hash_of_arrays => [
        {
          :name => "A database",
          :already_symbol => "localhost",
          :port => 1234
        }
      ]
    }

  end

end

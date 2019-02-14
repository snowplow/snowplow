# Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

  let(:c) { Cli.load_config(resource("sparse_config.yml"), "<<usage message>>") }
  let(:s) { Cli.load_config(resource("stream_config.yml"), "<<usage message>>") }
  let(:a) { { :debug => true, :include => [], :skip => [] } }

  def resource(name)
    filename = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s
    filename += "/"+name
  end

  describe '#load_targets' do
    it 'raises a ConfigError if two storage targets with identical ids are passed' do
      expect {
        Cli.load_targets(resource('invalid-targets'))
      }.to raise_exception(ConfigError, "Duplicate storage target ids: [\"id1\"]" )
    end
  end

  describe '#load_config' do
    it 'raises a ConfigError if the config file argument was nil' do
      expect {
        Cli.load_config(nil, "<<usage message>>")
      }.to raise_exception(ConfigError, "Missing option: config\n<<usage message>>" )
    end

    it 'raises a ConfigError if the config file argument could not be found' do
      expect {
        Cli.load_config("/no/such/file", "<<usage message>>")
      }.to raise_exception(ConfigError,
        "Configuration file '/no/such/file' does not exist, or is not a file\n<<usage message>>")
    end

    it 'raises a ConfigError if the config file argument is not a file' do
      expect {
        Cli.load_config("/tmp", "<<usage message>>")
      }.to raise_exception(ConfigError,
        "Configuration file '/tmp' does not exist, or is not a file\n<<usage message>>")
    end

    it 'sends back nil if the config is not required' do
      expect(Cli.load_config('/tmp', '', false)).to be_nil
    end
  end

  describe '#recursive_symbolize_keys' do
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
            "action2"
          ],
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

      expect(Cli.recursive_symbolize_keys(raw)).to eq({
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
      })
    end
  end

  describe '#add_trailing_slashes' do
    it 'adds trailing slashes to all buckets' do
      expect(Cli.add_trailing_slashes({
        '1' => 'a',
        '2' => {
          '3' => 'b',
          '4' => 'c'
        },
        '5' => ['d', [{
          '6' => 'e'
        }]]
      })).to eq({
        '1' => 'a/',
        '2' => {
          '3' => 'b/',
          '4' => 'c/'
        },
        '5' => ['d/', [{
          '6' => 'e/'
        }]]
      })
    end
  end

  describe '#validate_and_coalesce' do
    it 'should reject bogus resume_from' do
      a[:resume_from] = 'lunch'
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(ConfigError,
        "Invalid option: resume-from can be enrich, shred, elasticsearch, archive_raw, rdb_load, analyze, archive_enriched, archive_shredded, staging_stream_enrich not 'lunch'")

      %w(enrich shred elasticsearch archive_raw).each do |from|
        a[:resume_from] = from
        Cli.validate_and_coalesce(a, c)
      end
    end

    it 'should reject bogus skip' do
      a[:skip] = [ 'lunch' ]
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(ConfigError,
                                                                    "Invalid option: skip can be staging, enrich, shred, elasticsearch, archive_raw, rdb_load, consistency_check, analyze, load_manifest_check, archive_enriched, archive_shredded, staging_stream_enrich not 'lunch'")

      %w(enrich shred elasticsearch archive_raw).each do |from|
        a[:skip] = [ from ]
        Cli.validate_and_coalesce(a, c)
      end
    end

    it 'should config error if both resume and skip are present' do
      a[:resume_from] = 'enrich'
      a[:skip] = [ 'staging' ]
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(ConfigError,
        'resume-from and skip are mutually exclusive')
    end

    it 'should reject the collector format "invalid"' do
      c[:collectors][:format] = "invalid"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'invalid' not supported")
    end

    it 'should accept a valid tsv format in the form "tsv/something/something"' do
      c[:collectors][:format] = "tsv"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'tsv' not supported")
      c[:collectors][:format] = "tsv/something"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'tsv/something' not supported")
      c[:collectors][:format] = "tsv/something/"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'tsv/something/' not supported")
      c[:collectors][:format] = "tsv/something/something"
      Cli.validate_and_coalesce(a, c)
    end

    it 'should accept the thrift collector format' do
      c[:collectors][:format] = "thrift"
      Cli.validate_and_coalesce(a, c)
    end

    it 'should acept the clj-tomcat collector format' do
      c[:collectors][:format] = "clj-tomcat"
      Cli.validate_and_coalesce(a, c)
    end

    it 'should accept the cloudfront collector format' do
      c[:collectors][:format] = "cloudfront"
      Cli.validate_and_coalesce(a, c)
    end

    it 'should accept the json collector format' do
      c[:collectors][:format] = "json"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'json' not supported")
      c[:collectors][:format] = "json/something"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'json/something' not supported")
      c[:collectors][:format] = "json/something/"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'json/something/' not supported")
      c[:collectors][:format] = "json/something/something"
      Cli.validate_and_coalesce(a, c)
    end

    it 'should accept the ndjson collector format' do
      c[:collectors][:format] = "ndjson"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'ndjson' not supported")
      c[:collectors][:format] = "ndjson/something"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'ndjson/something' not supported")
      c[:collectors][:format] = "ndjson/something/"
      expect { Cli.validate_and_coalesce(a, c) }.to raise_exception(
        ConfigError, "collector_format 'ndjson/something/' not supported")
      c[:collectors][:format] = "ndjson/something/something"
      Cli.validate_and_coalesce(a, c)
    end

    it 'should accept stream enrich mode config' do
      Cli.validate_and_coalesce(a, s)
    end

    it 'should reject --skip staging in stream enrich mode' do
      a[:skip] = [ 'staging' ]
      expect {
        Cli.validate_and_coalesce(a, s)
      }.to raise_exception(ConfigError, "cannot skip staging nor enrich in stream enrich mode. Either skip staging_stream_enrich or resume from shred")
    end

    it 'should reject --resume-from enrich in stream enrich mode' do
      a[:resume_from] = 'enrich'
      expect {
        Cli.validate_and_coalesce(a, s)
      }.to raise_exception(ConfigError, "cannot resume from enrich in stream enrich mode")
    end

    it 'should reject --skip archive_raw in stream enrich mode' do
      a[:skip] = [ 'archive_raw' ]
      expect {
        Cli.validate_and_coalesce(a, s)
      }.to raise_exception(ConfigError, "cannot skip nor resume from archive_raw in stream enrich mode")
    end
  end
end

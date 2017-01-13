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

# Author::    Fred Blundun (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'spec_helper'

Runner = Snowplow::EmrEtlRunner::Runner
Cli = Snowplow::EmrEtlRunner::Cli

describe Runner do

  ConfigError = Snowplow::EmrEtlRunner::ConfigError

  def resource(name)
    filename = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s
    filename += "/"+name
  end

  def get_mock_config
    options = {
      :skip => [],
      :debug => false,
      :config_file => resource("sparse_config.yml"),
      :enrichments_directory => nil,
      :resolver_file => resource("iglu_resolver.json"),
      :start => nil,
      :end => nil,
      :skip => [],
      :process_enrich_location => nil,
      :process_shred_location => nil,
      :targets_directory => nil
    }

    args, config, enrichments, resolver, targets = Cli.process_options(options, nil)

    [args, config, enrichments, resolver, targets]
  end

  it 'should be able to get some mock config' do
    rgs, config, enrichments, resolver, tartgets = get_mock_config
    config[:collectors].should_not == nil
    config[:collectors][:format].should eq "cloudfront" # the sample is set to cloudfront
  end

  it 'adds trailing slashes to all buckets' do
    Runner.add_trailing_slashes({
      '1' => 'a',
      '2' => {
        '3' => 'b',
        '4' => 'c'
      },
      '5' => ['d', [{
        '6' => 'e'
      }]]
    }).should == {
      '1' => 'a/',
      '2' => {
        '3' => 'b/',
        '4' => 'c/'
      },
      '5' => ['d/', [{
        '6' => 'e/'
      }]]
    }
  end

  it 'should reject bogus skip stages' do
    args, config, enrichments, resolver, targets = get_mock_config
    args[:skip] = ["lunch"]
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "Invalid option: skip can be 'staging', 'emr', 'enrich', 'shred', 'elasticsearch', or 'archive_raw', not 'lunch'")
    puts targets.empty?

    %w(staging emr enrich shred elasticsearch archive_raw).each do |skip_arg|
      args[:skip] = [skip_arg]
      Runner.new args, config, enrichments, resolver, targets
    end
  end

  it 'should reject an end date before a start date' do
    args, config, enrichments, resolver, targets = get_mock_config
    args[:start] = "2015-12-11"
    args[:end] = "2015-11-11"
    expect{Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "Invalid options: end date '#{args[:end]}' is before start date '#{args[:start]}'")
  end

  it 'should reject start and end date if not using cloudfront' do
    args, config, enrichments, resolver, targets = get_mock_config
    args[:start] = "2015-12-11"
    args[:end] = "2015-12-12"
    config[:collectors][:format] = "clj-tomcat"
    expect{Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "--start and --end date arguments are only supported if collector_format is 'cloudfront'")
  end

  it 'should refuse to process shred and enrich together' do
    args, config, enrichments, resolver, targets = get_mock_config
    args[:process_shred_location] = "something"
    args[:process_enrich_location] = "something"
    expect{Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "Cannot process enrich and process shred, choose one")
  end

  it 'should reject the collector format "invalid"' do
    args, config, enrichments, resolver, targets = get_mock_config
    config[:collectors][:format] = "invalid"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'invalid' not supported")
  end

  it 'should accept a valid tsv format in the form "tsv/something/something"' do
    args, config, enrichments, resolver, targets = get_mock_config
    config[:collectors][:format] = "tsv"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'tsv' not supported")
    config[:collectors][:format] = "tsv/something"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'tsv/something' not supported")
    config[:collectors][:format] = "tsv/something/"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'tsv/something/' not supported")
    config[:collectors][:format] = "tsv/something/something"
    Runner.new args, config, enrichments, resolver, targets
  end

  it 'should accept the thrift collector format' do
    args, config, enrichments, resolver, targets = get_mock_config
    config[:collectors][:format] = "thrift"
    Runner.new args, config, enrichments, resolver, targets
  end

  it 'should acept the clj-tomcat collector format' do
    args, config, enrichments, resolver, targets = get_mock_config
    config[:collectors][:format] = "clj-tomcat"
    Runner.new args, config, enrichments, resolver, targets
  end

  it 'should accept the cloudfront collector format' do
    args, config, enrichments, resolver, targets = get_mock_config #defaults to cloudfront
    Runner.new args, config, enrichments, resolver, targets # this should work, args are legit
  end

  it 'should accept the json collector format' do
    args, config, enrichments, resolver, targets = get_mock_config
    config[:collectors][:format] = "json"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'json' not supported")
    config[:collectors][:format] = "json/something"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'json/something' not supported")
    config[:collectors][:format] = "json/something/"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'json/something/' not supported")
    config[:collectors][:format] = "json/something/something"
    Runner.new args, config, enrichments, resolver, targets
  end

  it 'should accept the ndjson collector format' do
    args, config, enrichments, resolver, targets = get_mock_config
    config[:collectors][:format] = "ndjson"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'ndjson' not supported")
    config[:collectors][:format] = "ndjson/something"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'ndjson/something' not supported")
    config[:collectors][:format] = "ndjson/something/"
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "collector_format 'ndjson/something/' not supported")
    config[:collectors][:format] = "ndjson/something/something"
    Runner.new args, config, enrichments, resolver, targets
  end

end

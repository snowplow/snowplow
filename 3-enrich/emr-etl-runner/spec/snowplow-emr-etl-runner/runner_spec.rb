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

describe Runner do

  ConfigError = Snowplow::EmrEtlRunner::ConfigError

  def resource(name)
    filename = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s
    filename += "/"+name
  end

  def get_mock_config
    options = {
      :debug => false,
      :config_file => resource("sparse_config.yml"),
      :enrichments_directory => nil,
      :resolver_file => resource("iglu_resolver.json"),
      :skip => [],
      :process_enrich_location => nil,
      :process_shred_location => nil,
      :targets_directory => nil,
      :include => []
    }

    args, config, enrichments, resolver, targets =
      Snowplow::EmrEtlRunner::Cli.process_options(options, nil, 'cmd')

    [args, config, enrichments, resolver, targets]
  end

  it 'should be able to get some mock config' do
    rgs, config, enrichments, resolver, targets = get_mock_config
    expect(config[:collectors]).not_to be_nil
    expect(config[:collectors][:format]).to eq("cloudfront") # the sample is set to cloudfront
  end

  it 'adds trailing slashes to all buckets' do
    expect(Runner.add_trailing_slashes({
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

  it 'should reject bogus skip stages' do
    args, config, enrichments, resolver, targets = get_mock_config
    args[:skip] = ["lunch"]
    expect {Runner.new args, config, enrichments, resolver, targets}.to raise_exception(ConfigError, "Invalid option: skip can be 'staging', 'emr', 'enrich', 'shred', 'elasticsearch', 'rdb_load', 'archive_raw' or 'analyze' not 'lunch'")
    puts targets.empty?

    %w(staging emr enrich shred elasticsearch archive_raw).each do |skip_arg|
      args[:skip] = [skip_arg]
      Runner.new args, config, enrichments, resolver, targets
    end
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

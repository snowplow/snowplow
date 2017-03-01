# Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2017 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'spec_helper'

Generator = Snowplow::EmrEtlRunner::Generator

describe Generator do

  class MockGenerator
    include Generator
  end

  subject { MockGenerator.new }

  let(:config) {
    filename = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/sparse_config.yml"
    Snowplow::EmrEtlRunner::Cli.load_config(filename, "")
  }

  let(:iglu) {
    path = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/iglu_resolver.json"
    File.open(path, 'rb') { |f| f.read }
  }

  describe '#generate' do
    it 'should take a config, a version and a filename as arguments' do
      expect(subject).to respond_to(:generate).with(4).argument
    end

    it 'should raise a RuntimeError since #get_schema_key is not implemented' do
      expect {
        subject.generate(config, iglu, 'v', 'f')
      }.to raise_error(RuntimeError,
        '#get_schema_key needs to be defined in all generators.')
    end
  end

  describe '#get_schema_key' do
    it 'should take a version as argument' do
      expect(subject).to respond_to(:get_schema_key).with(1).argument
    end

    it 'should raise a RuntimeError by default since get_schema_key is not impl' do
      expect {
        subject.get_schema_key('')
      }.to raise_error(RuntimeError,
        '#get_schema_key needs to be defined in all generators.')
    end
  end

  describe '#create_datum' do
    it 'should take a config as argument' do
      expect(subject).to respond_to(:create_datum).with(1).argument
    end

    it 'should raise a RuntimeError by default' do
      expect {
        subject.create_datum(config)
      }.to raise_error(RuntimeError, '#create_datum needs to be defined in all generators.')
    end
  end

end

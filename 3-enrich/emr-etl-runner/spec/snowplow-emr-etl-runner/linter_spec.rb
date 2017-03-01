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
require 'iglu-client'

Linter = Snowplow::EmrEtlRunner::Linter
LinterError = Snowplow::EmrEtlRunner::LinterError

describe Linter do

  def read_resource_file(filename)
    path = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/#{filename}"
    File.open(path, 'rb') { |f| f.read }
  end

  subject {
    json = JSON.parse(read_resource_file('iglu_resolver.json'), {:symbolize_names => true})
    Linter.new(Iglu::Resolver.parse(json))
  }

  describe '.initialize' do
    it 'should set the resolver field appropriately' do
      expect(subject.resolver).not_to be_nil
    end
  end

  describe '.lint' do
    it 'should create a Linter if the given config is properly formatted' do
      expect(Linter.lint(read_resource_file('iglu_resolver.json'))).to be_a(Linter)
    end
    it 'should give back a LinterError if the given config isnt properly formatted' do
      expect(Linter.lint('{"a":1}')).to eq(
        LinterError.new("Invalid resolver config {\"a\":1}: JSON instance is not " +
          "self-describing (schema property is absent):\n {\"a\":1}"))
    end
    it 'should give back a LinterError if the given config isnt json' do
      expect(Linter.lint('a')).to eq(
        LinterError.new("Could not parse resolver config a: unexpected token at 'a'"))
    end
  end

  describe '#lint_enrichments' do
    it 'should give back an empty Array of error if every enrichment is fine' do
      files = [ read_resource_file('enrichments/anon_ip.json'),
        read_resource_file('enrichments/cookie_extractor_config.json') ]
      expect(subject.lint_enrichments(files)).to eq([])
    end
    it 'should give back an Array made of the validation errors if the enrichments arent valid' do
      expect(subject.lint_enrichments(['{"a":1}', '{"b":2}'])).to eq([
        LinterError.new("Could not validate enrichment {\"a\":1}: JSON instance is not " +
          "self-describing (schema property is absent):\n {\"a\":1}"),
        LinterError.new("Could not validate enrichment {\"b\":2}: JSON instance is not " +
          "self-describing (schema property is absent):\n {\"b\":2}"),
      ])
    end
    it 'should give back an Array made of the validation errors if the enrichments arent jsons' do
      expect(subject.lint_enrichments(['a', 'b'])).to eq([
        LinterError.new("Could not validate enrichment a: unexpected token at 'a'"),
        LinterError.new("Could not validate enrichment b: unexpected token at 'b'"),
      ])
    end
  end
end

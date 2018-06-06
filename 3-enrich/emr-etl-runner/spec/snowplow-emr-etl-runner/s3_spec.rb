# Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2018 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'aws-sdk-s3'
require 'spec_helper'

S3 = Snowplow::EmrEtlRunner::S3

describe S3 do
  subject { Object.new.extend described_class }

  s3 = Aws::S3::Client.new(stub_responses: true)

  describe '#empty?' do
    it 'should take a client and location argument' do
      expect(subject).to respond_to(:empty?).with(2).argument
    end

    it 'should take a client, a location and a filter argument' do
      expect(subject).to respond_to(:empty?).with(3).argument
    end

    it 'should check a folder on S3 is empty' do
      s3.stub_responses(:list_objects_v2,
        { contents: [{ key: 'prefix/example1.jpg' }, { key: 'prefix/example2.jpg' }]})
      expect(subject.empty?(s3, 's3://bucket/prefix')).to eq(false)
    end

    it 'should filter folders by default' do
      s3.stub_responses(:list_objects_v2, { contents: [{ key: 'prefix/folder/' }]})
      expect(subject.empty?(s3, 's3://bucket/prefix')).to eq(true)
    end

    it 'should filter $folder$ files by default' do
      s3.stub_responses(:list_objects_v2, { contents: [{ key: 'prefix/something_$folder$' }]})
      expect(subject.empty?(s3, 's3://bucket/prefix')).to eq(true)
    end

    it 'should support custom filters' do
      s3.stub_responses(:list_objects_v2, { contents: [{ key: 'abc' }]})
      expect(subject.empty?(s3, 's3://bucket/prefix', lambda { |k| k.length == 3})).to eq(false)
    end
  end

  describe '#list_object_names' do
    it 'should take a client, a location and a filter argument' do
      expect(subject).to respond_to(:list_object_names).with(3).argument
    end

    it 'should filter file names based on the filter' do
      s3.stub_responses(:list_objects_v2, { contents: [{ key: 'abc' }, { key: 'defg' }]})
      expect(subject.list_object_names(s3, 's3://bucket/prefix', lambda { |k| k.length == 3}))
        .to eq(['abc'])
    end

  end

  describe '#parse_bucket_prefix' do
    it 'should take a s3 url argument' do
      expect(subject).to respond_to(:parse_bucket_prefix).with(1).argument
    end

    it 'should parse the bucket and prefix' do
      expect(subject.parse_bucket_prefix('s3://bucket/prefix')).to eq(['bucket', 'prefix'])
      expect(subject.parse_bucket_prefix('s3://bucket/prefix/file.jpg')).to eq(
        ['bucket', 'prefix/file.jpg'])
    end
  end
end

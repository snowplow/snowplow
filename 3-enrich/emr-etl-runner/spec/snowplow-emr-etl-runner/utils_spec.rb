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
require 'base64'
require 'json'

Utils = Snowplow::EmrEtlRunner::Utils

describe Utils do
  subject { Object.new.extend described_class }

  describe '#build_enrichments_json' do
    it { should respond_to(:build_enrichments_json).with(1).argument }

    it 'should build a json with empty data if given an empty array' do
      expect(Base64.strict_decode64(subject.build_enrichments_json([]))).to eq({
        'schema' => 'iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0',
        'data'   => []
      }.to_json)
    end

    it 'should build the proper json if given another json' do
      expect(Base64.strict_decode64(subject.build_enrichments_json([ '{"a": "1" }' ]))).to eq({
        'schema' => 'iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0',
        'data'   => [ { 'a' => '1' } ]
      }.to_json)
    end
  end

  describe '#get_hosted_assets_bucket' do
    it { should respond_to(:get_hosted_assets_bucket).with(3).argument }

    it 'should give back the standard bucket if in eu-west-1' do
      expect(subject.get_hosted_assets_bucket('bucket', 'bucket', 'eu-west-1')).to eq('bucket/')
    end

    it 'should suffix the standard bucket with the region if not in eu-west-1' do
      expect(subject.get_hosted_assets_bucket('bucket', 'bucket', 'eu-central-1'))
        .to eq('bucket-eu-central-1/')
    end

    it 'should give back the specified bucket if in eu-west-1' do
      expect(subject.get_hosted_assets_bucket('std-bucket', 'bucket', 'eu-west-1')).to eq('bucket/')
    end

    it 'should give back the specified bucket if not in eu-west-1' do
      expect(subject.get_hosted_assets_bucket('std-bucket', 'bucket', 'eu-central-1'))
        .to eq('bucket/')
    end
  end

  describe '#get_cc_version' do
    it { should respond_to(:get_cc_version).with(1).argument }

    it 'should give back 1.5 if the enrich version is <= 1.8.0' do
      expect(subject.get_cc_version("0.6.0")).to eq("1.5")
      expect(subject.get_cc_version("1.8.0")).to eq("1.5")
    end

    it 'should give back 1.10 if the enrich version is > 1.8.0' do
      expect(subject.get_cc_version("1.9.0")).to eq("1.10")
    end

    it 'should fail if the argument is not a version' do
      expect {
        subject.get_cc_version("s")
      }.to raise_error(ArgumentError, 'Malformed version number string s')
    end
  end

  describe '#is_ua_ndjson' do
    it { should respond_to(:is_ua_ndjson).with(1).argument }

    it 'should give back true for an airship fmt' do
      expect(subject.is_ua_ndjson('ndjson/com.urbanairship.connect/blabla')).to be(true)
    end

    it 'should give back false for another fmt' do
      expect(subject.is_ua_ndjson('thrift')).to be(false)
    end
  end

  describe '#is_cloudfront_log' do
    it { should respond_to(:is_cloudfront_log).with(1).argument }

    it 'should give back true for a cloudfront fmt' do
      expect(subject.is_cloudfront_log('cloudfront')).to be(true)
      expect(subject.is_cloudfront_log('tsv/com.amazon.aws.cloudfront/blabla')).to be(true)
    end

    it 'should give back false for another fmt' do
      expect(subject.is_cloudfront_log('ndjson/com.urbanairship.connect/blabla'))
    end
  end

  describe '#get_s3_endpoint' do
    it { should respond_to(:get_s3_endpoint).with(1).argument }

    it 'should prefix the s3 url with the region if its not us-east-1' do
      expect(subject.get_s3_endpoint('eu-west-1')).to eq('s3-eu-west-1.amazonaws.com')
    end

    it 'should give back s3.amazonaws.com if us-east-1' do
      expect(subject.get_s3_endpoint('us-east-1')).to eq('s3.amazonaws.com')
    end
  end

  describe '#get_assets' do
    it { should respond_to(:get_assets).with(5).argument }

    it 'should give back the old enrich artifact if major == 0' do
      expect(subject.get_assets('/', '0.1.0', '1.2.0', '0.1.0', '0.1.0')).to eq({
        :enrich => '/3-enrich/hadoop-etl/snowplow-hadoop-etl-0.1.0.jar',
        :shred => '/3-enrich/scala-hadoop-shred/snowplow-hadoop-shred-1.2.0.jar',
        :loader   => "/4-storage/rdb-loader/snowplow-rdb-loader-0.1.0.jar",
        :elasticsearch => '/4-storage/hadoop-elasticsearch-sink/hadoop-elasticsearch-sink-0.1.0.jar'
      })
    end

    it 'should give back the new enrich artifact if major > 0' do
      expect(subject.get_assets('/', '1.4.0', '1.2.0', '0.1.0', '0.1.0')).to eq({
        :enrich => '/3-enrich/scala-hadoop-enrich/snowplow-hadoop-enrich-1.4.0.jar',
        :shred => '/3-enrich/scala-hadoop-shred/snowplow-hadoop-shred-1.2.0.jar',
        :loader   => "/4-storage/rdb-loader/snowplow-rdb-loader-0.1.0.jar",
        :elasticsearch => '/4-storage/hadoop-elasticsearch-sink/hadoop-elasticsearch-sink-0.1.0.jar'
      })
    end
  end

  describe '#partition_by_run' do
    it { should respond_to(:partition_by_run).with(3).argument }

    it 'should give back the folder with the run id appended if retain' do
      expect(subject.partition_by_run('f', 'id')).to eq('frun=id/')
    end

    it 'should give back nil if not retain' do
      expect(subject.partition_by_run('f', 'id', false)).to be_nil
    end
  end

  describe '#output_codec_from_compression_format' do
    it { should respond_to(:output_codec_from_compression_format).with(1).argument }

    it 'should give back an empty array if the compression format is nil' do
      expect(subject.output_codec_from_compression_format(nil)).to eq([])
    end

    it 'should give back an empty array if the compression format is NONE' do
      expect(subject.output_codec_from_compression_format('NONE')).to eq([])
    end

    it 'should give back gz if the compression format is GZIP or GZ' do
      expect(subject.output_codec_from_compression_format('GZIP')).to eq([ '--outputCodec', 'gz' ])
      expect(subject.output_codec_from_compression_format('GZ')).to eq([ '--outputCodec', 'gz' ])
    end

    it 'should return the proper output codec if the provided one is supported' do
      expect(subject.output_codec_from_compression_format('LZO')).to eq([ '--outputCodec', 'lzo' ])
    end
  end

  describe '#glob_path' do
    it { should respond_to(:glob_path).with(1).argument }

    it 'should add /* if not already present' do
      expect(subject.glob_path('p')).to eq('p/*')
      expect(subject.glob_path('p/')).to eq('p/*')
    end

    it 'should do nothing if the path already ends with /*' do
      expect(subject.glob_path('p/*')).to eq('p/*')
    end
  end
end

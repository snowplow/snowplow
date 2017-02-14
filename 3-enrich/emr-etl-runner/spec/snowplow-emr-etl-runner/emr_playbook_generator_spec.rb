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

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'spec_helper'

EmrPlaybookGenerator = Snowplow::EmrEtlRunner::EmrPlaybookGenerator

describe EmrPlaybookGenerator do

  subject { EmrPlaybookGenerator.new }

  let(:c) {
    filename = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/sparse_config.yml"
    Snowplow::EmrEtlRunner::Cli.load_config(filename, "")
  }

  let(:iglu) {
    path = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/iglu_resolver.json"
    File.open(path, 'rb') { |f| f.read }
  }

  it { should be_a Snowplow::EmrEtlRunner::Generator }

  it 'should write a proper playbook file' do
    filename = '/tmp/playbook.json'
    subject.generate(c, iglu, '1-0-0', filename, true)
    expect(File.exist?(filename)).to be true
  end

  describe '#get_schema_key' do
    it { should respond_to(:get_schema_key).with(1).argument }

    it 'should give back the proper schema' do
      expect(subject.get_schema_key('1-0-0')).to eq(
        Iglu::SchemaKey.new('com.snowplowanalytics.dataflowrunner', 'PlaybookConfig', 'avro',
          Iglu::SchemaVer.new(1, 0, 0)))
    end
  end

  describe '#create_datum' do
    it 'should add region and credentials to get_steps' do
      expect(subject.create_datum(c, false,
          [ 'enrich', 'shred', 'elasticsearch', 's3distcp', 'archive_raw' ], '', [])).to eq({
        "region" => "eu-west-1",
        "credentials" => {
          "accessKeyId" => "SAMPLE KEY",
          "secretAccessKey" => "SAMPLE SECRET KEY"
        },
        "steps" => []
      })
    end
  end

  describe '#get_steps' do
    it 'should give back no steps if everything is false' do
      expect(subject.send(:get_steps, c, false, false, false, false, false, false, '', []))
        .to eq([])
    end

    it 'should give back only the debug step if only debug is true' do
      expect(subject.send(:get_steps, c, true, false, false, false, false, false, '', [])).to eq([{
        "type" => "CUSTOM_JAR",
        "name" => "Setup Hadoop debugging",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar",
        "arguments" => [ "s3://eu-west-1.elasticmapreduce/libs/state-pusher/0.1/fetch" ]
      }])
    end

    it 'should give back only the enrich step if only enrich is true' do
      res = subject.send(:get_steps, c, false, true, false, false, false, false, '', [])
      expect(res.length).to eq(1)
      expect(res[0]).to include({
        "type" => "CUSTOM_JAR",
        "name" => "Enrich raw events",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "spha/3-enrich/scala-hadoop-enrich/snowplow-hadoop-enrich-1.3.0.jar",
        "arguments" => [
          "com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob",
          "--hdfs", "--input_format", "cloudfront", "--etl_tstamp", be_a(String),
          "--iglu_config", "", "--enrichments",
          "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9lbnJpY2htZW50cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W119",
          "--input_folder", "rp", "--output_folder", be_a(String), "--bad_rows_folder", be_a(String)
        ]
      })
    end

    it 'should only give back the shred step if only shred is true' do
      res = subject.send(:get_steps, c, false, false, true, false, false, false, '', [])
      expect(res.length).to eq(1)
      expect(res[0]).to include({
        "type" => "CUSTOM_JAR",
        "name" => "Shred enriched events",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "spha/3-enrich/scala-hadoop-shred/snowplow-hadoop-shred-0.6.0.jar",
        "arguments" => [
          "com.snowplowanalytics.snowplow.enrich.hadoop.ShredJob", "--hdfs", "--iglu_config", "",
          "--input_folder", "eg/*", "--output_folder", be_a(String), "--bad_rows_folder", be_a(String)
        ]
      })
    end

    it 'should only give back the shred and es steps if only es and shred are true' do
      res = subject.send(:get_steps, c, false, false, true, false, true, false, '', [])
      expect(res.length).to eq(2)
      expect(res[1]).to include(
        "type" => "CUSTOM_JAR",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "spha/4-storage/hadoop-elasticsearch-sink/hadoop-elasticsearch-sink-0.1.0.jar",
        "arguments" => [
          "com.snowplowanalytics.snowplow.storage.hadoop.ElasticsearchJob",
          "--input", be_a(String), "--host", "h", "--port", "9", "--index", "d", "--type", "t",
          "--es_nodes_wan_only", "false", "--delay", "60"
        ]
      )
    end

    it 'should only give back the archive step if only archive_raw is true' do
      res = subject.send(:get_steps, c, false, false, false, false, false, true, '', [])
      expect(res.length).to eq(1)
      expect(res[0]).to include(
        "type" => "CUSTOM_JAR",
        "name" => "S3DistCp: raw S3 staging -> S3 archive",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
        "arguments" => [ "--src", "rp", "--dest", be_a(String),
          "--s3Endpoint", "s3-eu-west-1.amazonaws.com", "--deleteOnSuccess" ]
      )
    end
  end

  describe '#get_es_steps' do
    it 'should give back no steps if enrich and shred are false' do
      expect(subject.send(:get_es_steps, c, false, false, 'j', 'i')).to eq([])
    end

    it 'should give back the es steps' do
      expect(subject.send(:get_es_steps, c, true, true, 'j', 'i')).to eq([
        {
          "type" => "CUSTOM_JAR",
          "name" => "Errors in ebrun=i/ -> Elasticsearch: es",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "j",
          "arguments" => [ "com.snowplowanalytics.snowplow.storage.hadoop.ElasticsearchJob",
            "--input", "ebrun=i/", "--host", "h", "--port", "9", "--index", "d", "--type", "t",
            "--es_nodes_wan_only", "false", "--delay", "60" ]
        },
        {
          "type" => "CUSTOM_JAR",
          "name" => "Errors in sbrun=i/ -> Elasticsearch: es",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "j",
          "arguments" => [ "com.snowplowanalytics.snowplow.storage.hadoop.ElasticsearchJob",
            "--input", "sbrun=i/", "--host", "h", "--port", "9", "--index", "d", "--type", "t",
            "--es_nodes_wan_only", "false" ]
        }
      ])
    end
  end

  describe '#get_shred_steps' do
    it 'should build only the shred if s3distcp is false' do
      expect(subject.send(:get_shred_steps, c, false, 's3e', false, true, 'j', 's', 'f', 'i', 'r'))
          .to eq([{
        "type" => "CUSTOM_JAR",
        "name" => "Shred enriched events",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "j",
        "arguments" => [ "com.snowplowanalytics.snowplow.enrich.hadoop.ShredJob",
          "--hdfs", "--iglu_config", "cg==",
          "--input_folder", "s/*", "--output_folder", "sgrun=i/", "--bad_rows_folder", "sbrun=i/" ]
      }])
    end

    it 'should add a final s3distcp step is s3distcp is true' do
      expect(subject.send(:get_shred_steps, c, false, 's3e', true, true, 'j', 's', 'f', 'i', 'r'))
        .to eq([
        {
          "type" => "CUSTOM_JAR",
          "name" => "Shred enriched events",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "j",
          "arguments" => [ "com.snowplowanalytics.snowplow.enrich.hadoop.ShredJob",
            "--hdfs", "--iglu_config", "cg==", "--input_folder", "s/*",
            "--output_folder", "hdfs:///local/snowplow/shredded-events/",
            "--bad_rows_folder", "sbrun=i/" ]
        },
        {
          "type" => "CUSTOM_JAR",
          "name" => "S3DistCp: shredded HDFS -> S3",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
          "arguments" => [ "--src", "hdfs:///local/snowplow/shredded-events/", "--dest", "sgrun=i/",
            "--s3Endpoint", "s3e", "--srcPattern", ".*part-.*", "--outputCodec", "none" ]
        }
      ])
    end

    it 'should prefix another s3distcp step if enrich is false and s3distcp is true' do
      expect(subject.send(:get_shred_steps, c, false, 's3e', true, false, 'j', 's', 'f', 'i', 'r'))
        .to eq([
        {
          "type" => "CUSTOM_JAR",
          "name" => "S3DistCp: enriched S3 -> HDFS",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
          "arguments" => [ "--src", "f", "--dest", "s",
            "--s3Endpoint", "s3e", "--srcPattern", ".*part-.*" ]
        },
        {
          "type" => "CUSTOM_JAR",
          "name" => "Shred enriched events",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "j",
          "arguments" => [ "com.snowplowanalytics.snowplow.enrich.hadoop.ShredJob",
            "--hdfs", "--iglu_config", "cg==", "--input_folder", "s/*",
            "--output_folder", "hdfs:///local/snowplow/shredded-events/",
            "--bad_rows_folder", "sbrun=i/" ]
        },
        {
          "type" => "CUSTOM_JAR",
          "name" => "S3DistCp: shredded HDFS -> S3",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
          "arguments" => [ "--src", "hdfs:///local/snowplow/shredded-events/", "--dest", "sgrun=i/",
            "--s3Endpoint", "s3e", "--srcPattern", ".*part-.*", "--outputCodec", "none" ]
        }
      ])
    end
  end

  describe '#get_enrich_steps' do
    it 'should build only the enrich step is s3distcp is false' do
      expect(subject.send(:get_enrich_steps, c, false, 's3e', false, 'j', 's', 'f', 'i', '1', 'r',
        [])).to eq([{
        "type" => "CUSTOM_JAR",
        "name" => "Enrich raw events",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "j",
        "arguments" => [ "com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob",
          "--hdfs", "--input_format", "cloudfront", "--etl_tstamp", "1",
          "--iglu_config", "cg==", "--enrichments",
          "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9lbnJpY2htZW50cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W119",
          "--input_folder", "rp", "--output_folder", "s", "--bad_rows_folder", "ebrun=i/" ]
      }])
    end

    it 'should build all necessary steps is s3distcp is true' do
      expect(subject.send(:get_enrich_steps, c, false, 's3e', true, 'j', 's', 'f', 'i', '1', 'r',
        [])).to eq([
        {
          "type" => "CUSTOM_JAR",
          "name" => "S3DistCp: raw S3 -> HDFS",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
          "arguments" => [ "--src", "rp", "--dest", "hdfs:///local/snowplow/raw-events/",
            "--s3Endpoint", "s3e", "--groupBy", ".*\\.([0-9]+-[0-9]+-[0-9]+)-[0-9]+\\..*",
            "--targetSize", "128", "--outputCodec", "lzo" ]
        },
        {
          "type" => "CUSTOM_JAR",
          "name" => "Enrich raw events",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "j",
          "arguments" => [ "com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob",
            "--hdfs", "--input_format", "cloudfront", "--etl_tstamp", "1",
            "--iglu_config", "cg==", "--enrichments",
            "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9lbnJpY2htZW50cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W119",
            "--input_folder", "hdfs:///local/snowplow/raw-events/", "--output_folder", "s",
            "--bad_rows_folder", "ebrun=i/" ]
        },
        {
          "type" => "CUSTOM_JAR",
          "name" => "S3DistCp: enriched HDFS -> S3",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
          "arguments" => [ "--src", "s", "--dest", "f", "--s3Endpoint", "s3e",
            "--srcPattern", ".*part-.*", "--outputCodec", "none" ]
        },
        {
          "type" => "CUSTOM_JAR",
          "name" => "S3DistCp: enriched HDFS _SUCCESS -> S3",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
          "arguments" => [ "--src", "s", "--dest", "f",
            "--s3Endpoint", "s3e", "--srcPattern", ".*_SUCCESS" ]
        }
      ])
    end
  end

  describe '#get_debugging_step' do
    it 'should build the proper step' do
      expect(subject.send(:get_debugging_step, 'r')).to eq({
        "type" => "CUSTOM_JAR",
        "name" => "Setup Hadoop debugging",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "s3://r.elasticmapreduce/libs/script-runner/script-runner.jar",
        "arguments" => [ "s3://r.elasticmapreduce/libs/state-pusher/0.1/fetch" ]
      })
    end
  end

  describe '#get_scalding_step' do
    it 'should have the default args' do
      expect(subject.send(:get_scalding_step, 'n', 'j', 'm',
        { in: 'i', good: 'g', bad: 'b', errors: 'e' })).to eq({
          "type" => "CUSTOM_JAR",
          "name" => "n",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "j",
          "arguments" => [ "m", "--hdfs", "--input_folder", "i", "--output_folder", "g",
            "--bad_rows_folder", "b", "--exceptions_folder", "e" ]
        })
    end

    it 'should rm the nil folders' do
      expect(subject.send(:get_scalding_step, 'n', 'j', 'm',
        { in: nil, good: 'g', bad: nil, errors: 'e' })).to eq({
          "type" => "CUSTOM_JAR",
          "name" => "n",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "j",
          "arguments" => [ "m", "--hdfs", "--output_folder", "g", "--exceptions_folder", "e" ]
        })
    end

    it 'should add extra args' do
      expect(subject.send(:get_scalding_step, 'n', 'j', 'm',
        { in: nil, good: 'g', bad: nil, errors: 'e' }, [ 'a' ])).to eq({
          "type" => "CUSTOM_JAR",
          "name" => "n",
          "actionOnFailure" => "CANCEL_AND_WAIT",
          "jar" => "j",
          "arguments" => [ "m", "--hdfs", "a", "--output_folder", "g", "--exceptions_folder", "e" ]
        })
    end
  end

  describe '#get_s3distcp_step' do
    it 'should use the old jar if legacy' do
      expect(subject.send(:get_s3distcp_step, true, 'n', 's', 'd', 'e')).to eq({
        "type" => "CUSTOM_JAR",
        "name" => "n",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "/home/hadoop/lib/emr-s3distcp-1.0.jar",
        "arguments" => [ "--src", "s", "--dest", "d", "--s3Endpoint", "e" ]
      })
    end

    it 'should use the new jar if not legacy' do
      expect(subject.send(:get_s3distcp_step, false, 'n', 's', 'd', 'e')).to eq({
        "type" => "CUSTOM_JAR",
        "name" => "n",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
        "arguments" => [ "--src", "s", "--dest", "d", "--s3Endpoint", "e" ]
      })
    end

    it 'should add extra params if provided' do
      expect(subject.send(:get_s3distcp_step, false, 'n', 's', 'd', 'e', [ '--deleteOnSuccess' ])).to eq({
        "type" => "CUSTOM_JAR",
        "name" => "n",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
        "arguments" => [ "--src", "s", "--dest", "d", "--s3Endpoint", "e", "--deleteOnSuccess" ]
      })
    end
  end

  describe '#get_hbase_step' do
    it 'should create a proper hbase step' do
      expect(subject.send(:get_hbase_step, '1.8.0')).to eq({
        "type" => "CUSTOM_JAR",
        "name" => "Start HBase 1.8.0",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "/home/hadoop/lib/hbase-1.8.0.jar",
        "arguments" => [ "emr.hbase.backup.Main", "--start-master" ]
      })
    end
  end

  describe '#get_custom_jar_step' do
    it 'should create a proper hash' do
      expect(subject.send(:get_custom_jar_step, 'n', 'j', [ 'a' ])).to eq({
        "type" => "CUSTOM_JAR",
        "name" => "n",
        "actionOnFailure" => "CANCEL_AND_WAIT",
        "jar" => "j",
        "arguments" => [ "a" ]
      })
    end
  end
end

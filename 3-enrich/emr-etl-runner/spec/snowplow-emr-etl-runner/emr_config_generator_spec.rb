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
require 'fileutils'
require 'iglu-client'

EmrConfigGenerator = Snowplow::EmrEtlRunner::EmrConfigGenerator

describe EmrConfigGenerator do

  subject { EmrConfigGenerator.new }

  let(:config) {
    filename = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/sparse_config.yml"
    Snowplow::EmrEtlRunner::Cli.load_config(filename, "")
  }

  let(:iglu) {
    path = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/iglu_resolver.json"
    File.open(path, 'rb') { |f| f.read }
  }

  it { should be_a Snowplow::EmrEtlRunner::Generator }

  describe '#generate' do
    it { should respond_to(:generate).with(4).argument }

    it 'should write a proper emr cluster file' do
      filename = '/tmp/cluster.json'
      subject.generate(config, iglu, '1-0-0', filename)
      ref_file = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s + "/cluster.json"
      expect(FileUtils.cmp(ref_file, filename)).to be true
    end
  end

  describe '#get_schema_key' do
    it { should respond_to(:get_schema_key).with(1).argument }

    it 'should give back the proper schema' do
      expect(subject.get_schema_key('1-0-0')).to eq(
        Iglu::SchemaKey.new('com.snowplowanalytics.dataflowrunner', 'ClusterConfig', 'avro',
          Iglu::SchemaVer.new(1, 0, 0)))
    end
  end

  describe '#create_datum' do
    it 'should create the proper record given sparse_config' do
      expect(subject.create_datum(config)).to eq({
        "name" => "test file (invalid)",
        "logUri" => "s3://not-a-bucket",
        "region" => "eu-west-1",
        "credentials" => {
          "accessKeyId" => "SAMPLE KEY",
          "secretAccessKey" => "SAMPLE SECRET KEY"
        },
        "roles" => {
          "jobflow" => "EMR_EC2_DefaultRole",
          "service" => "EMR_DefaultRole"
        },
        "ec2" => {
          "amiVersion" => "4.7.0",
          "keyName" => "sample",
          "location" => { "vpc" => { "subnetId" => "E" } },
          "instances" => {
            "master" => { "type" => "m1.medium" },
            "core" => {
              "type" => "m1.medium",
              "count" => 2,
              "ebsConfiguration" => {
                "ebsOptimized" => false,
                "ebsBlockDeviceConfigs" => [
                  {
                    "volumesPerInstance" => 1,
                    "volumeSpecification" => {
                      "iops" => 1,
                      "sizeInGB" => 500,
                      "volumeType" => "gp2"
                    }
                  }
                ]
              }
            },
            "task" => {
              "type" => "m1.medium",
              "count" => 0,
              "bid" => "0.015"
            }
          }
        },
        "tags" => [],
        "bootstrapActionConfigs" => [
          {
            "name" => "Bootstrap action (ami bootstrap script)",
            "scriptBootstrapAction" => {
              "path" => "s3://snowplow-hosted-assets/common/emr/snowplow-ami4-bootstrap-0.2.0.sh",
              "args" => [ "1.10" ]
            }
          }
        ],
        "configurations" => [
          { "classification" => "core-site", "properties" => { "io.file.buffer.size" => "65536" } },
          { "classification" => "mapred-site", "properties" => { "mapreduce.user.classpath.first" => "true" } }
        ],
        "applications" => [ "Hadoop", "Spark" ]
      })
    end
  end

  describe '#get_tags' do
    it 'should give back an empty array given an empty hash' do
      expect(subject.send(:get_tags, {})).to eq([])
    end
    it 'should turn the hash into an array (k, v) pairs' do
      expect(subject.send(:get_tags, { a: 1, b: 2})).to eq([
        { "key" => "a", "value" => "1" },
        { "key" => "b", "value" => "2" }
      ])
    end
  end

  describe '#get_location_hash' do
    it 'should give back a vpc hash if using subnet' do
      expect(subject.send(:get_location_hash, 'subnet', 'placement'))
        .to eq({ "vpc" => { "subnetId" => "subnet" } })
      expect(subject.send(:get_location_hash, 'subnet', nil))
        .to eq({ "vpc" => { "subnetId" => "subnet" } })
    end

    it 'should give back a classic hash if using placement' do
      expect(subject.send(:get_location_hash, nil, 'placement'))
        .to eq({ "classic" => { "availabilityZone" => "placement" } })
    end
  end

  describe '#get_configurations' do
    it 'should give back an empty array if legacy' do
      expect(subject.send(:get_configurations,  Gem::Version.new("3.9.0"))).to eq([])
    end

    it 'should give back an array of config if not legacy' do
      expect(subject.send(:get_configurations, Gem::Version.new("4.7.0"))).to eq([
        { "classification" => "core-site", "properties" => { "io.file.buffer.size" => "65536" } },
        { "classification" => "mapred-site", "properties" => { "mapreduce.user.classpath.first" => "true" } }
      ])
    end
  end

  describe '#get_ebs_configuration' do
    it 'should give back an empty hash if the config is nil' do
      expect(subject.send(:get_ebs_configuration, nil)).to eq({})
    end
    
    it 'should give back a proper hash otherwise' do
      hash = {
        :ebs_optimized => true,
        :volume_type => "io1",
        :volume_iops => 12,
        :volume_size => 20
      }
      expect(subject.send(:get_ebs_configuration, hash)).to eq ({
        "ebsOptimized" => true,
        "ebsBlockDeviceConfigs" => [
          {
            "volumesPerInstance" => 1,
            "volumeSpecification" => {
              "iops" => 12,
              "sizeInGB" => 20,
              "volumeType" => "io1"
            }
          }
        ]
      })
    end
  end

  describe '#get_ami_action' do
    it 'should give back a 0.1.0 script if legacy' do
      expect(subject.send(:get_ami_action, Gem::Version.new("3.8.0"), 'eu-west-1', '1.8.0')).to eq({
        "name" => "Bootstrap action (ami bootstrap script)",
        "scriptBootstrapAction" => {
          "path" => "s3://snowplow-hosted-assets/common/emr/snowplow-ami3-bootstrap-0.1.0.sh",
          "args" => [ "1.5" ]
        }
      })
    end

    it 'should give back a 0.2.0 script if not legacy' do
      expect(subject.send(:get_ami_action, Gem::Version.new("4.1.0"), 'eu-west-1', '1.9.0')).to eq({
        "name" => "Bootstrap action (ami bootstrap script)",
        "scriptBootstrapAction" => {
          "path" => "s3://snowplow-hosted-assets/common/emr/snowplow-ami4-bootstrap-0.2.0.sh",
          "args" => [ "1.10" ]
        }
      })
    end

    it 'should give back am ami5 script if AMI is greater than 5' do
      expect(subject.send(:get_ami_action, Gem::Version.new("5.1.0"), 'eu-west-1', '1.9.0')).to eq({
        "name" => "Bootstrap action (ami bootstrap script)",
        "scriptBootstrapAction" => {
          "path" => "s3://snowplow-hosted-assets/common/emr/snowplow-ami5-bootstrap-0.1.0-rc1.sh",
          "args" => [ "1.10" ]
        }
      })
    end
  end

  describe '#get_action' do
    it 'should give back the proper actions without arguments' do
      expect(subject.send(:get_action, 'name', 'path')).to eq({
        "name" => "name",
        "scriptBootstrapAction" => {
          "path" => "path",
          "args" => []
        }
      })
    end

    it 'should give back the proper actions with arguments' do
      expect(subject.send(:get_action, 'name', 'path', [ 'a' ])).to eq({
        "name" => "name",
        "scriptBootstrapAction" => {
          "path" => "path",
          "args" => [ "a" ]
        }
      })
    end
  end

  describe '#get_bootstrap_actions' do
    it 'should build a list of actions starting with no actions (thrift + legacy)' do
      expect(subject.send(:get_bootstrap_actions,
        [], 'thrift', Gem::Version.new('3.8.0'), 'eu-west-1', '1.8.0')).to eq([
          {
            "name" => "Hadoop bootstrap action (buffer size)",
            "scriptBootstrapAction" => {
              "path" => "s3n://elasticmapreduce/bootstrap-actions/configure-hadoop",
              "args" => [ "-c", "io.file.buffer.size=65536" ]
            }
          },
          {
            "name" => "Hadoop bootstrap action (user cp first)",
            "scriptBootstrapAction" => {
              "path" => "s3n://elasticmapreduce/bootstrap-actions/configure-hadoop",
              "args" => [ "-m", "mapreduce.user.classpath.first=true" ]
            }
          },
          {
            "name" => "Bootstrap action (ami bootstrap script)",
            "scriptBootstrapAction" => {
              "path" => "s3://snowplow-hosted-assets/common/emr/snowplow-ami3-bootstrap-0.1.0.sh",
              "args" => [ "1.5" ]
            }
          }
        ])
    end

    it 'should incorporate the specified list of actions' do
      expect(subject.send(:get_bootstrap_actions, [
        { "name" => "something", "scriptBootstrapAction" => { "path" => "path", "args" => [] } },
        { "name" => "another thing", "scriptBootstrapAction" => { "path" => "another path", "args" => [] } },
      ], 'not-thrift', Gem::Version.new('4.8.0'), 'eu-west-1', '1.9.0')).to eq([
        { "name" => "something", "scriptBootstrapAction" => { "path" => "path", "args" => [] } },
        { "name" => "another thing", "scriptBootstrapAction" => { "path" => "another path", "args" => [] } },
        {
          "name" => "Bootstrap action (ami bootstrap script)",
          "scriptBootstrapAction" => {
            "path" => "s3://snowplow-hosted-assets/common/emr/snowplow-ami4-bootstrap-0.2.0.sh",
            "args" => [ "1.10" ]
          }
        }
      ])
    end

  end
end

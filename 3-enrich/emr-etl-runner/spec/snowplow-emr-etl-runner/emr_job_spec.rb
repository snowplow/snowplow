# Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

require 'spec_helper'

EmrJob = Snowplow::EmrEtlRunner::EmrJob

describe EmrJob do

  describe '#initialise' do

    it 'should set basic jobflow configuration from config hash' do

      test_region = "ap-east-1"
      test_name = "test name"
      test_key = "test key"
      test_job_role = "test job role"
      test_service_role = "test service role"
      test_info = "some additional info"
      test_security_config = "some security config"
      test_log_uri = "s3://my-log-bucket"

      jobflow = setup_jobflow { |config|
        config[:aws][:emr][:region] = test_region
        config[:aws][:emr][:jobflow][:job_name] = test_name
        config[:aws][:emr][:ec2_key_name] = test_key
        config[:aws][:emr][:jobflow_role] = test_job_role
        config[:aws][:emr][:service_role] = test_service_role
        config[:aws][:emr][:additional_info] = test_info
        config[:aws][:emr][:security_configuration] = test_security_config
        config[:aws][:s3][:buckets][:log] = test_log_uri
      }

      expect(jobflow.region).to eq(test_region)
      expect(jobflow.name).to eq(test_name)
      expect(jobflow.ec2_key_name).to eq(test_key)
      expect(jobflow.job_flow_role).to eq(test_job_role)
      expect(jobflow.service_role).to eq(test_service_role)
      expect(jobflow.additional_info).to eq(test_info)
      expect(jobflow.security_configuration).to eq(test_security_config)
      expect(jobflow.log_uri).to eq(test_log_uri)

    end

    it 'should set ami and release for old and new versions' do

      legacy_jobflow = setup_jobflow { |config|
        config[:aws][:emr][:ami_version] = "3.0.0"
      }

      modern_jobflow = setup_jobflow { |config|
        config[:aws][:emr][:ami_version] = "4.0.0"
      }

      expect(legacy_jobflow.ami_version).to eq("3.0.0")
      expect(legacy_jobflow.release_label).to eq(nil)

      expect(modern_jobflow.ami_version).to eq(nil)
      expect(modern_jobflow.release_label).to eq('emr-4.0.0')

    end

    it 'should set placement or subnet' do

      test_placement = "ap-east-1c"
      test_subnet_id = "subnet-0123456789abcdef"

      with_placement = setup_jobflow { |config|
        config[:aws][:emr][:placement] = test_placement
        config[:aws][:emr][:ec2_subnet_id] = nil
      }

      with_subnet = setup_jobflow { |config|
        config[:aws][:emr][:placement] = nil
        config[:aws][:emr][:ec2_subnet_id] = test_subnet_id
      }

      expect(with_placement.placement).to eq(test_placement)
      expect(with_placement.ec2_subnet_id).to eq(nil)

      expect(with_subnet.placement).to eq(nil)
      expect(with_subnet.ec2_subnet_id).to eq(test_subnet_id)

    end

    it 'should configure instance groups' do

      test_master_instance_type = 'cr1.8xlarge'

      test_core_instance_type = 'z1d.12xlarge'
      test_core_instance_count = 42
      test_core_instance_bid = 0.42
      test_core_instance_ebs = {
        :volume_size => 420,
        :volume_type => 'io1',
        :volume_iops => 4200,
        :ebs_optimized => false
      }

      test_task_instance_type = 'c5n.18xlarge'
      test_task_instance_count = 3142
      test_task_instance_bid = 3.142

      jobflow = setup_jobflow { |config|
        config[:aws][:emr][:jobflow][:master_instance_type] = test_master_instance_type

        config[:aws][:emr][:jobflow][:core_instance_type] = test_core_instance_type
        config[:aws][:emr][:jobflow][:core_instance_count] = test_core_instance_count
        config[:aws][:emr][:jobflow][:core_instance_bid] = test_core_instance_bid
        config[:aws][:emr][:jobflow][:core_instance_ebs] = test_core_instance_ebs

        config[:aws][:emr][:jobflow][:task_instance_type] = test_task_instance_type
        config[:aws][:emr][:jobflow][:task_instance_count] = test_task_instance_count
        config[:aws][:emr][:jobflow][:task_instance_bid] = test_task_instance_bid
      }

      igs = jobflow.instance_variable_get(:@instance_groups)

      expect(igs[:master].type).to eq(test_master_instance_type)
      expect(igs[:master].count).to eq(1)

      expect(igs[:core].type).to eq(test_core_instance_type)
      expect(igs[:core].count).to eq(test_core_instance_count)
      expect(igs[:core].bid_price).to eq(test_core_instance_bid.to_s)
      expect(igs[:core].ebs_configuration.ebs_block_device_configs[0].volume_type).to eq(test_core_instance_ebs[:volume_type])
      expect(igs[:core].ebs_configuration.ebs_block_device_configs[0].size_in_gb).to eq(test_core_instance_ebs[:volume_size])
      expect(igs[:core].ebs_configuration.ebs_block_device_configs[0].iops).to eq(test_core_instance_ebs[:volume_iops])
      expect(igs[:core].ebs_configuration.ebs_optimized).to eq(test_core_instance_ebs[:ebs_optimized])

      expect(igs[:task].type).to eq(test_task_instance_type)
      expect(igs[:task].count).to eq(test_task_instance_count)
      expect(igs[:task].bid_price).to eq(test_task_instance_bid.to_s)

    end

  end

  private 

  def setup_jobflow

    debug = false
    staging = false
    enrich = false
    staging_stream_enrich = false
    shred = false
    es = false
    archive_raw = false
    rdb_load = false
    archive_enriched = 'pipeline'
    archive_shredded = 'pipeline'
    enrichments_array = []
    targets = {:DUPLICATE_TRACKING => nil, :FAILED_EVENTS => [], :ENRICHED_EVENTS => []}
    rdbloader_steps = {:skip => [], :include => []}
    use_persistent_jobflow = false
    persistent_jobflow_duration = '5h'

    config = Snowplow::EmrEtlRunner::Cli.load_config(resource("stream_config.yml"), "<<usage message>>")
    yield config
    resolver = File.read(resource("iglu_resolver.json"))
    EmrJob.new(debug, staging, enrich, staging_stream_enrich, shred, es, archive_raw, rdb_load, archive_enriched, archive_shredded, config, enrichments_array, resolver, targets, rdbloader_steps, use_persistent_jobflow, persistent_jobflow_duration)
      .instance_variable_get(:@jobflow)
  end

  def resource(name)
    filename = File.expand_path(File.dirname(__FILE__)+"/resources/").to_s
    filename += "/"+name
  end

end

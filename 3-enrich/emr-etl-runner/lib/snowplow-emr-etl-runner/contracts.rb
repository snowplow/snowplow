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

# Author::    Alex Dean (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2015 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'contracts'
require 'iglu-client'

module Snowplow
  module EmrEtlRunner

    include Contracts

    C = Contracts

    CompressionFormat = lambda { |s| %w(NONE GZIP).include?(s) }
    VolumeTypes = lambda { |s| %w(standard gp2 io1).include?(s) }
    PositiveInt = lambda { |i| i.is_a?(Integer) && i > 0 }

    # The Hash containing assets for Hadoop.
    AssetsHash = ({
      :enrich  => Maybe[String],
      :shred   => String,
      :loader  => String,
      :elasticsearch => String
      })

    # The Hash of the CLI arguments.
    ArgsHash = ({
      :debug => Bool,
      :resume_from => Maybe[String],
      :skip => Maybe[ArrayOf[String]],
      :include => Maybe[ArrayOf[String]],
      :lock => Maybe[String],
      :consul => Maybe[String],
      :ignore_lock_on_start => Maybe[Bool],
      :use_persistent_jobflow => Maybe[Bool]
      })

    # The Hash containing the buckets field from the configuration YAML
    BucketHash = ({
      :assets => String,
      :jsonpath_assets => Maybe[String],
      :log => String,
      :encrypted => Bool,
      :raw => Maybe[({
        :in => ArrayOf[String],
        :processing => String,
        :archive => String
        })],
      :enriched => ({
        :good => String,
        :bad => Maybe[String],
        :errors => Maybe[String],
        :archive => Maybe[String],
        :stream => Maybe[String]
        }),
      :shredded => ({
        :good => String,
        :bad => String,
        :errors => Maybe[String],
        :archive => Maybe[String]
        })
      })

    # The Hash containing the configuration for a core instance using EBS.
    CoreInstanceEbsHash = ({
      :volume_size => PositiveInt,
      :volume_type => VolumeTypes,
      :volume_iops => Maybe[PositiveInt],
      :ebs_optimized => Maybe[Bool]
      })

    # The Hash containing effectively the configuration YAML.
    ConfigHash = ({
      :aws => ({
        :access_key_id => String,
        :secret_access_key => String,
        :s3 => ({
          :region => String,
          :buckets => BucketHash
          }),
        :emr => ({
          :ami_version => String,
          :region => String,
          :jobflow_role => String,
          :service_role => String,
          :placement => Maybe[String],
          :ec2_subnet_id => Maybe[String],
          :ec2_key_name => String,
          :security_configuration => Maybe[String],
          :bootstrap => Maybe[ArrayOf[String]],
          :software => ({
            :hbase => Maybe[String],
            :lingual => Maybe[String]
            }),
          :jobflow => ({
            :job_name => String,
            :master_instance_type => String,
            :core_instance_count => Num,
            :core_instance_type => String,
            :core_instance_ebs => Maybe[CoreInstanceEbsHash],
            :task_instance_count => Num,
            :task_instance_type => String,
            :task_instance_bid => Maybe[Num]
            }),
          :additional_info => Maybe[String],
          :bootstrap_failure_tries => Num,
          :configuration => Maybe[HashOf[Symbol, HashOf[Symbol, String]]]
          }),
        }),
      :collectors => Maybe[({
        :format => String,
        })],
      :enrich => ({
        :versions => Maybe[({
          :spark_enrich => String
          })],
        :continue_on_unexpected_error => Maybe[Bool],
        :output_compression => CompressionFormat
        }),
      :storage => ({
        :versions => ({
          :rdb_shredder => String,
          :hadoop_elasticsearch => String,
          :rdb_loader => String
          })
        }),
      :monitoring => ({
        :tags => HashOf[Symbol, String],
        :logging => ({
          :level => String
          }),
        :snowplow => Maybe[{
          :method => String,
          :collector => String,
          :app_id => String
          }]
        })
      })

    # Path and content of JSON file
    JsonFileHash = ({
        :file => String,
        :json => Hash[Symbol, nil]
    })

    # The Array (Tuple3) containing the CLI arguments, configuration YAML, and configuration JSONs
    ArgsConfigEnrichmentsResolverTuple = [String, ArgsHash, Maybe[ConfigHash], ArrayOf[String], String, ArrayOf[JsonFileHash]]

    # Storage targets grouped by purpose
    TargetsHash = ({
        :DUPLICATE_TRACKING => Maybe[Iglu::SelfDescribingJson],
        :FAILED_EVENTS => ArrayOf[Iglu::SelfDescribingJson],
        :ENRICHED_EVENTS => ArrayOf[Iglu::SelfDescribingJson]
    })

    # archive_{enriched,shredded} can be either run as:
    # recover - without previous steps, archive latest run_id
    # pipeline - following the enrich and rdb_load, with known run_id
    # skip - don't archive
    ArchiveStep = C::Or['recover', 'pipeline', 'skip']

    RdbLoaderSteps = ({
      :skip => ArrayOf[String],
      :include => ArrayOf[String]
    })

    # Record of all possible steps that can be launched during EMR job
    EmrSteps = ({
      :staging => Bool,
      :enrich => Bool,
      :staging_stream_enrich => Bool,
      :shred => Bool,
      :es => Bool,
      :archive_raw => Bool,
      :rdb_load => Bool,
      :consistency_check => Bool,
      :load_manifest_check => Bool,
      :analyze => Bool,
      :archive_enriched => Bool,
      :archive_shredded => Bool
    })
  end
end

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

# Author::    Alex Dean (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'contracts'
include Contracts

module Snowplow
  module EmrEtlRunner

    # The Hash containing assets for Hadoop.
    AssetsHash = ({
      :maxmind  => String,
      :s3distcp => String,
      :hadoop   => String
      })

    # The Hash of the CLI arguments.
    ArgsHash = ({
      :debug => Bool,
      :start => Maybe[String],
      :end => Maybe[String],
      :skip => Maybe[ArrayOf[String]],
      :process_bucket => Maybe[String]
      })

    # The Hash for the IP anonymization enrichment.
    AnonIpHash = ({
      :enabled => Bool,
      :anon_octets => Num
      })

    # The Hash containing effectively the configuration YAML.
    ConfigHash = ({
      :logging => ({
        :level => String
        }),
      :aws => ({
        :access_key_id => String,
        :secret_access_key => String
        }),
      :s3 => ({
        :region => String,
        :buckets => ({
          :assets => String,
          :log => String,
          :in => String,
          :processing => String,
          :out => String,
          :out_bad_rows => String,
          :out_errors => Maybe[String],
          :archive => String
          })
        }),
      :emr => ({
        :ami_version => String,
        :region => String,
        :placement => Maybe[String],
        :ec2_subnet_id => Maybe[String],
        :ec2_key_name => String,
        :jobflow => ({
          :master_instance_type => String,
          :core_instance_count => Num,
          :core_instance_type => String,
          :task_instance_count => Num,
          :task_instance_type => String,
          :task_instance_bid => Maybe[Num]
          })
        }),
      :etl => ({
        :job_name => String,
        :hadoop_etl_version => String,
        :collector_format => String,
        :continue_on_unexpected_error => Bool
        }),
      :enrichments => ({
        :anon_ip => AnonIpHash
        })
      })

    # The Array (Tuple2) containing the CLI arguments and configuration YAML.
    ArgsConfigTuple = [ArgsHash, ConfigHash]

  end
end
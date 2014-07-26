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
      :enrich  => String,
      :shred   => String
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

    # The Hash for the Iglu client config
    IgluConfigHash = ({
      :schema => String,
      :data => ({
        :cache_size => Num,
        :repositories => ArrayOf[({
          :name => String,
          :priority => Num,
          :vendor_prefixes => ArrayOf[String],          
          :connection => ({
            :http => ({
              :uri => String
              })
            })
          })]
        })
      })

    # The Hash containing the buckets field from the configuration YAML
    BucketHash = ({
      :assets => String,
      :log => String,
      :raw => ({
        :in => String,
        :processing => String
        }),
      :enriched => ({
        :good => String,
        :bad => String,
        :errors => Maybe[String]
        }),
      :shredded => ({
        :good => String,
        :bad => String,
        :errors => Maybe[String]
        })
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
        :buckets => BucketHash
        }),
      :emr => ({
        :ami_version => String,
        :region => String,
        :placement => Maybe[String],
        :ec2_subnet_id => Maybe[String],
        :ec2_key_name => String,
        :software => ({
          :hbase => Maybe[String],
          :lingual => Maybe[String]
          }),
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
        :versions => ({
          :hadoop_enrich => String,
          :hadoop_shred => String
          }),
        :collector_format => String,
        :continue_on_unexpected_error => Bool
        }),
      :iglu => IgluConfigHash
      })

    # The Array (Tuple3) containing the CLI arguments, configuration YAML, and configuration JSONs
    ArgsConfigEnrichmentsTuple = [ArgsHash, ConfigHash, ArrayOf[String]]

  end
end

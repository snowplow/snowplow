# Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'aws/s3'

# Ruby module to support the two S3-related actions required by
# the daily ETL job:
# 1. Uploading the daily-etl.q HiveQL query to S3
# 2. Archiving the CloudFront log files by moving them into a separate bucket
module S3Tasks

  class NoBucketError < StandardError; end

  # Uploads the Hive query to S3 ready to be executed as part of the Hive job.
  # Ensures we are executing the most recent version of the Hive query.
  #
  # Parameters:
  # +config+:: the hash of configuration options
  #
  # TODO: replace with Elasticity code when added:
  # - https://github.com/rslifka/elasticity/issues/34
  # - https://github.com/rslifka/elasticity/pull/35
  def sync_assets(config)

   # Connect to S3
    AWS::S3::Base.establish_connection!(
      :access_key_id     => config[:aws][:access_key_id],
      :secret_access_key => config[:aws][:secret_access_key]
    )

    # Upload the two query files and the serde
    # Array of files to upload: "tuple" format is [Filename, Local filepath, S3 bucket path, Content type]
    [[config[:daily_query_file], config[:daily_query_path], config[:s3][:buckets][:assets], 'text/plain'],
     [config[:datespan_query_file], config[:datespan_query_path], config[:s3][:buckets][:assets], 'text/plain'],
     [config[:serde_file], config[:serde_path], config[:s3][:buckets][:serde], 'application/java-archive']
    ].each do |f|
      AWS::S3::S3Object.store(f[0], open(f[1]), f[2], :content_type => f[3])
    end
  end
  module_function :sync_assets

  # Moves (archives) the processed CloudFront logs to an archive bucket.
  # Prevents the same log files from being processed again.
  # Parameters:
  # +config+:: the hash of configuration options
  def archive_logs(config)

    s3 = Fog::Storage.new({
      :provider => 'AWS',
      :aws_access_key_id => config[:aws][:access_key_id],
      :aws_secret_access_key => config[:aws][:secret_access_key]
    })

    # Bucket containing logs to archive...
    inbucket = bucket(config[:s3][:buckets][:in])
    inbucket.files.each do |file|
      puts "File %s last modified: %s" % [file.key, file.last_modified]
    end

    # TODO: implement
    puts "Archiving CloudFront logs... (TODO)"
  end
  module_function :archive_logs

  def bucket(bucket_name)
    index = s3.directories.index { |d| d.key == bucket_name }
    if index
      s3.directories[index]
    else
      raise NoBucketError, "Bucket '%s' does not exist" % bucket_name
    end
  end

end
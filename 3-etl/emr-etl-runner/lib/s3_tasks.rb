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
require 'fog'


# Ruby module to support the two S3-related actions required by
# the daily ETL job:
# 1. Uploading the daily-etl.q HiveQL query to S3
# 2. Archiving the CloudFront log files by moving them into a separate bucket
module S3Tasks

  class NoBucketError < StandardError; end
  class DirectoryNotEmptyError < StandardError; end

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


  # Moves new CloudFront logs to a processing bucket.
  #
  # Parameters:
  # +config+:: the hash of configuration options
  def stage_logs_for_emr(config)

    puts 'Staging CloudFront logs...'

    s3 = Fog::Storage.new({
      :provider => 'AWS',
      :aws_access_key_id => config[:aws][:access_key_id],
      :aws_secret_access_key => config[:aws][:secret_access_key]
    })

    in_config = config[:s3][:buckets][:in].match('^(.+?)/(.+)/$');
    in_bucket = in_config[1]
    in_dir = in_config[2]

    processing = config[:s3][:buckets][:processing].match('^(.+?)/(.+)/$');
    processing_bucket = processing[1]
    processing_dir = processing[2]

    # check whether our processing directory is empty
    if s3.directories.get(processing_bucket, :prefix => processing_dir).files().length > 1
      raise DirectoryNotEmptyError, "The processing directory is not empty"
    end

    dates = []
    Date.parse(config[:start]).upto(Date.parse(config[:end])) do |day|
      dates << day.strftime('%Y-%m-%d')
    end

    # find files with the given date
    target_file = '^.+/([^/]+(' + dates.join('|') + ')[^/]+\.gz)$'

    s3.directories.get(in_bucket, :prefix => in_dir).files().each{ |file|
      if m = file.key.match(target_file)
        #puts 'Staging '+ m[1]
        file.copy(processing_bucket, processing_dir + '/' + m[1])
        file.destroy()
      end
    }
  end
  module_function :stage_logs_for_emr


  # Moves (archives) the processed CloudFront logs to an archive bucket.
  # Prevents the same log files from being processed again.
  # Parameters:
  # +config+:: the hash of configuration options
  def archive_logs(config)

    puts 'Archiving CloudFront logs...'

    s3 = Fog::Storage.new({
      :provider => 'AWS',
      :aws_access_key_id => config[:aws][:access_key_id],
      :aws_secret_access_key => config[:aws][:secret_access_key]
    })

    processing = config[:s3][:buckets][:processing].match('^(.+?)/(.+)/$');
    processing_bucket = processing[1]
    processing_dir = processing[2]

    archive = config[:s3][:buckets][:archive].match('^(.+?)/(.+)/$');
    archive_bucket = archive[1]
    archive_dir = archive[2]

    s3.directories.get(processing_bucket, :prefix => processing_dir).files().each{ |file|
      if m = file.key.match('[^/]+\.(\d\d\d\d-\d\d-\d\d)-\d\d\.[^/]+\.gz$')
        filename = m[0]
        date = m[1]
        #puts 'Archiving ' + filename
        file.copy(archive_bucket, archive_dir + '/' + date + '/' + filename)
        file.destroy()
      end
    }
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
# Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2013 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'sluice'

# Ruby module to support the S3-related actions required by
# the Hive-based ETL process.
module SnowPlow
  module EmrEtlRunner
    module S3Tasks

      # Constants for CloudFront log format
      CF_DATE_FORMAT = '%Y-%m-%d'
      CF_FILE_EXT = '.gz'

      # Moves new CloudFront logs to a processing bucket.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      def stage_logs_for_emr(config)
        puts 'Staging CloudFront logs...'

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Get S3 locations
        in_location = Sluice::Storage::S3::Location.new(config[:s3][:buckets][:in])
        processing_location = Sluice::Storage::S3::Location.new(config[:s3][:buckets][:processing])

        # Check whether our processing directory is empty
        unless Sluice::Storage::S3::is_empty?(s3, processing_location)
          raise DirectoryNotEmptyError, "The processing directory is not empty"
        end

        # Move the files we need to move (within the date span)
        files_to_move = case
        when (config[:start].nil? and config[:end].nil?)
          if config[:etl][:collector_format] == 'clj-tomcat'
            '.*localhost\_access\_log\.txt\-.*'
          else
            '.+'
          end
        when config[:start].nil?
          Sluice::Storage::files_up_to(config[:end], CF_DATE_FORMAT, CF_FILE_EXT)
        when config[:end].nil?
          Sluice::Storage::files_from(config[:start], CF_DATE_FORMAT, CF_FILE_EXT)
        else
          Sluice::Storage::files_between(config[:start], config[:end], CF_DATE_FORMAT, CF_FILE_EXT)
        end

        # Hive ignores files which begin with underscores
        strip_underscore = lambda { |filepath|
          if m = filepath.match('^_+(.*\.gz)$')
            return m[1]
          else
            return filepath
          end
        }

        Sluice::Storage::S3::move_files(s3, in_location, processing_location, files_to_move, strip_underscore, true)

        # Wait for s3 to eventually become consistant
        puts "Waiting a minute to allow S3 to settle (eventual consistency)"
        sleep(60)

      end
      module_function :stage_logs_for_emr

      # Moves (archives) the processed CloudFront logs to an archive bucket.
      # Prevents the same log files from being processed again.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      def archive_logs(config)
        puts 'Archiving CloudFront logs...'

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Get S3 locations
        processing_location = Sluice::Storage::S3::Location.new(config[:s3][:buckets][:processing]);
        archive_location = Sluice::Storage::S3::Location.new(config[:s3][:buckets][:archive]);

        # Attach date path if filenames include datestamp
        add_date_path = lambda { |filepath|
          if m = filepath.match('[^/]+\.(\d\d\d\d-\d\d-\d\d)-\d\d\.[^/]+\.gz$')
            filename = m[0]
            date = m[1]
            return date + '/' + filename
          else
            return filepath
          end
        }

        # Move all the files in the Processing Bucket
        Sluice::Storage::S3::move_files(s3, processing_location, archive_location, '.+', add_date_path)

      end
      module_function :archive_logs

    end
  end
end
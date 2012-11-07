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

require 'sluice'

# Ruby module to support the S3-related actions required by
# the Hive-based ETL process.
module SnowPlow
  module StorageLoader
    module S3Tasks

      # Moves SnowPlow event files to the Processing Bucket.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      def stage_events(config)
        puts 'Staging SnowPlow events...'

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Get S3 locations
        in_location = Sluice::Storage::S3::Location.new(config[:s3][:buckets][:in])
        processing_location = Sluice::Storage::S3::Location.new(config[:s3][:buckets][:processing])

        # Check whether our Processing Bucket is empty
        if Sluice::Storage::S3::is_empty?(s3, processing_location)
          raise DirectoryNotEmptyError, "The processing directory is not empty"
        end

        Sluice::Storage::S3::move_files(s3, in_location, processing_location, '.+')

        # Wait for S3 to eventually become consistant
        puts "Waiting a minute to allow S3 to settle (eventual consistency)"
        sleep(60)

      end
      module_function :stage_events

      # Downloads the SnowPlow event files from the Processing
      # Bucket to the local filesystem, ready to be loaded
      # into different storage options.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      def download_events(config)
        puts "Downloading SnowPlow events..."

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # TODO: implement the rest of this method
        puts "TODO"

      end
      module_function :download_events

      # Moves (archives) the loaded SnowPlow event files to the
      # Archive Bucket.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      def archive_events(config)
        puts 'Archiving SnowPlow events...'

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Get S3 locations
        processing_location = Sluice::Storage::S3::Location.new(config[:s3][:buckets][:processing]);
        archive_location = Sluice::Storage::S3::Location.new(config[:s3][:buckets][:archive]);

        # Move all the files in the Processing Bucket
        Sluice::Storage::S3::move_files(s3, processing_location, archive_location, '.+', add_date_path)

      end
      module_function :archive_events

    end
  end
end
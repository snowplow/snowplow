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

require 'sluice'

# Ruby module to support the S3-related actions required by
# the Hive-based ETL process.
module Snowplow
  module StorageLoader
    module S3Tasks

      # We ignore the Hadoop success files
      EMPTY_FILES = "_SUCCESS"
      NON_EMPTY_FILES = Sluice::Storage::NegativeRegex.new(EMPTY_FILES)

      # Downloads the Snowplow event files from the In
      # Bucket to the local filesystem, ready to be loaded
      # into different storage options.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      def download_events(config)
        puts "Downloading Snowplow events..."

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:aws][:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])
        s3.host = region_to_safe_host([:aws][:s3][:region])

        # Get S3 location of In Bucket plus local directory
        in_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:shredded][:good])
        download_dir = config[:storage][:download][:folder]

        # Download
        Sluice::Storage::S3::download_files(s3, in_location, download_dir, NON_EMPTY_FILES)

        nil
      end
      module_function :download_events

      # Moves (archives) the loaded Snowplow event files to the
      # Archive Bucket.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      def archive_files(config)
        puts 'Archiving Snowplow events...'

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:aws][:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Get S3 locations
        archive_files_of_type(s3, config, :enriched)
        archive_files_of_type(s3, config, :shredded) unless config[:skip].include?('shred')
      
        nil
      end
      module_function :archive_files

    private

      # Moves (archives) a set of files: either enriched events
      # or shredded types
      #
      # Parameters:
      # +s3+:: the S3 connection
      # +config+:: the hash of configuration options
      # +file_type+:: the type of files (a symbol)
      def archive_files_of_type(s3, config, file_type)

        # Check we have shredding configured
        good_path = config[:aws][:s3][:buckets][file_type][:good]
        return nil if file_type == :shredded and good_path.nil?

        # Get S3 locations
        good_location = Sluice::Storage::S3::Location.new(good_path)
        archive_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][file_type][:archive])

        # Move all the files of this type
        # First move all data files
        Sluice::Storage::S3::move_files(s3, good_location, archive_location, NON_EMPTY_FILES)
        # Eventual consistency
        sleep(60)
        # Then move the _SUCCESS flag file
        Sluice::Storage::S3::move_files(s3, good_location, archive_location, EMPTY_FILES)

        nil
      end
      module_function :archive_files_of_type

      # Forces Fog to use the Northern Virginia endpoint
      # (s3-external-1.amazonaws.com), which has better
      # read-after-write properties than the global
      # endpoint (s3.amazonaws.com).
      #
      # We return nil for any other region to delegate
      # back to Fog for the most appropriate host (Fog
      # has its own region_to_host method).
      #
      # Parameters:
      # +region+:: the AWS region
      def region_to_safe_host(region)
        case region.to_s
        when 'us-east-1', ''
          's3-external-1.amazonaws.com'
        else
          nil
        end
      end
      module_function :region_to_safe_host

    end
  end
end
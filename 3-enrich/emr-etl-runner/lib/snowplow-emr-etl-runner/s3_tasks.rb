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

require 'date'
require 'sluice'
require 'contracts'

# Ruby module to support the S3-related actions required by
# the Hive-based ETL process.
module Snowplow
  module EmrEtlRunner
    module S3Tasks

      include Contracts

      # Constants for CloudFront log format
      CF_DATE_FORMAT = '%Y-%m-%d'
      CF_FILE_EXT = '.gz'

      # Cleans up our filenames, making archiving
      # much cleaner. See the test suite for
      # expected behaviors.
      #
      # Lambda factory required for unit tests.
      #
      # Parameters:
      # +region+:: the region to add into the filenames
      Contract String => Func[String, String => String]
      def self.build_fix_filenames(region)
        return lambda { |basename, filepath|
          # Prepend sub-dir to prevent one set of files
          # from overwriting same-named in other sub-dir
          if filepath_match = filepath.match('([^/]+)/[^/]+$')
            instance = filepath_match[1]

            extn = File.extname(basename)
            name = File.basename(basename, extn)

            # This will convert Beanstalk epoch timestamps to our CloudFront-like yyyy-MM-dd-HH
            final_name, final_extn =
              if name_match = name.match(/^_*(.*)\.txt-?([[:digit:]]+)$/)
                base, tstamp = name_match.captures
                begin
                  tstamp_ymdh = Time.at(tstamp.to_i).utc.to_datetime.strftime("%Y-%m-%d-%H")
                  [ base + '.' + tstamp_ymdh, '.txt' + extn ]
                rescue StandardError => e
                  [ name, extn ]
                end
              else
                [ name, extn ]
              end

            # Hopefully basename.yyyy-MM-dd-HH.region.instance.txt.gz
            return final_name + '.' + region + '.' + instance + final_extn
          else
            # Hadoop ignores files which begin with underscores
            if m = basename.match('^_+(.*\.gz)$')
              return m[1]
            else
              return basename
            end
          end
        }
      end

      # Moves new CloudFront logs to a processing bucket.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      #
      # Returns true if file(s) were staged
      Contract ArgsHash, ConfigHash => Bool
      def self.stage_logs_for_emr(args, config)
        Logging::logger.debug 'Staging CloudFront logs...'

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:aws][:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Get S3 locations
        in_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:raw][:in])
        processing_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:raw][:processing])

        # Check whether our processing directory is empty
        unless Sluice::Storage::S3::is_empty?(s3, processing_location)
          raise DirectoryNotEmptyError, "The processing directory is not empty"
        end

        # Early check whether our enrichment directory is empty. We do a late check too
        unless args[:skip].include?('emr') or args[:skip].include?('enrich')
          enriched_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:enriched][:good])
          unless Sluice::Storage::S3::is_empty?(s3, enriched_location)
            raise DirectoryNotEmptyError, "Should not stage files for enrichment, #{enriched_location} is not empty"
          end
        end

        # Early check whether our shred directory is empty. We do a late check too
        unless args[:skip].include?('emr') or args[:skip].include?('shred')
          shred_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:shredded][:good])
          unless Sluice::Storage::S3::is_empty?(s3, shred_location)
            raise DirectoryNotEmptyError, "Should not stage files for shredding, #{shred_location} is not empty"
          end
        end

        # Move the files we need to move (within the date span)
        files_to_move = case
        when (args[:start].nil? and args[:end].nil?)
          if config[:collectors][:format] == 'clj-tomcat'
            '.*localhost\_access\_log.*\.txt.*'
          else
            '.+'
          end
        when args[:start].nil?
          Sluice::Storage::files_up_to(args[:end], CF_DATE_FORMAT, CF_FILE_EXT)
        when args[:end].nil?
          Sluice::Storage::files_from(args[:start], CF_DATE_FORMAT, CF_FILE_EXT)
        else
          Sluice::Storage::files_between(args[:start], args[:end], CF_DATE_FORMAT, CF_FILE_EXT)
        end

        fix_filenames = build_fix_filenames(config[:aws][:s3][:region])
        files_moved = Sluice::Storage::S3::move_files(s3, in_location, processing_location, files_to_move, fix_filenames, true)

        if files_moved.length == 0
          false
        else
          # Wait for s3 to eventually become consistent
          Logging::logger.debug "Waiting a minute to allow S3 to settle (eventual consistency)"
          sleep(60)

          true
        end
      end

      # Moves (archives) the processed CloudFront logs to an archive bucket.
      # Prevents the same log files from being processed again.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      Contract ConfigHash => nil
      def self.archive_logs(config)
        Logging::logger.debug 'Archiving CloudFront logs...'

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:aws][:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Get S3 locations
        processing_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:raw][:processing]);
        archive_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:raw][:archive]);

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
        nil
      end

    end
  end
end
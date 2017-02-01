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

      RETRY_EXT = 'retry'

      # Generate a short UrbanAirship filename
      #
      # Parameters:
      # +nested_file_path+:: The full path name as provided by UrbanAirship in their S3 bucket
      Contract String => String
      def self.generate_short_ua_filename(nested_file_path)
        if matches = nested_file_path.match(/([^\.\/.]+)\/(.+)\/S3_JSON\/.+\/([^\..]+)/)
          ua_appkey, integration_id, filename = matches.captures
          return "urbanairship.#{filename}.#{ua_appkey}.#{integration_id}.ndjson"
        else
          raise "short filename could not be generated - wrong format detected (source:#{nested_file_path})"
        end
      end

      # Cleans up our filenames, making archiving
      # much cleaner. See the test suite for
      # expected behaviors.
      #
      # Lambda factory required for unit tests.
      #
      # Parameters:
      # +region+:: the region to add into the filenames
      # +collector_format+:: the format of the collector log files
      # Contract String, String => Func[String, String => String] disabled contract because of:
      # https://github.com/egonSchiele/contracts.ruby/issues/238
      def self.build_fix_filenames(region, collector_format)
        return lambda { |basename, filepath|

          if matches = collector_format.match(/^.+?\/(.+?)\//)
            adapter = matches.captures[0]
            if adapter == "com.urbanairship.connect"
              return generate_short_ua_filename(filepath)
            end
          end

          # Prepend sub-dir to prevent one set of files
          # from overwriting same-named in other sub-dir
          if filepath_match = filepath.match('([^/]+)/[^/]+$')
            instance = filepath_match[1]

            extn = File.extname(basename)
            name = File.basename(basename, extn)

            # This will convert Beanstalk epoch timestamps to our CloudFront-like yyyy-MM-dd-HH
            final_name, final_extn =get_final_name_and_extension( filepath, basename, instance)

            Monitoring::Logging::logger.debug "Instance = #{instance}, final_name:'#{final_name}', final_extn:'#{final_extn}'"

            return (final_extn.eql? RETRY_EXT) ?
                (RETRY_EXT + '/' + final_name ) :
                ( final_name + '.' + region + '.' + instance + final_extn ) # Hopefully basename.yyyy-MM-dd-HH.region.instance.txt.gz
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

      # convert Beanstalk epoch timestamps to our CloudFront-like yyyy-MM-dd-HH
      # It will also do the validation of the converted timestamp doing a reverse conversion.
      # dst is taken into account
      #
      # Parameters:
      # +filepath+:: instance + filename string format
      # +instance+:: instance id
      # +basename+:: full filename
      def self.get_final_name_and_extension(filepath, basename, instance)

        extn = File.extname(basename)
        name = File.basename(basename, extn)

        if name_match = name.match(/^_*(.*)\.txt-?([[:digit:]]+)$/)
          base, tstamp = name_match.captures
          begin
            allowed_tstamp_delta = 2700 # 45 minutes. Sometimes AWS EB sets late timestamps
            max_retries = 5
            rebuilt_timestamp = 0
            tstamp_ymdh=String.new
            retries = 0
            tstamp_delta = 0
            both_tstamps_match = false

            loop do
              tstamp_ymdh = Time.at(tstamp.to_i).utc.to_datetime.strftime("%Y-%m-%d-%H")
              # Reverse timestamp conversion. We validate the converted tstamp_ymdh is equals to tstamp
              # This takes into account the "Day light saving time"
              rebuilt_timestamp = (Time.parse(tstamp_ymdh.reverse.sub(/-/, ' ').reverse).to_i + Time.zone_offset(Time.at(tstamp.to_i).zone).to_i).to_i
              tstamp_delta = Time.at(rebuilt_timestamp).dst? ? (tstamp.to_i - rebuilt_timestamp.to_i - 3600) : (tstamp.to_i - rebuilt_timestamp.to_i) # Day light saving time hack
              both_tstamps_match = tstamp_delta < allowed_tstamp_delta

              Monitoring::Logging::logger.debug "Instance = #{instance}, Timestamp = #{tstamp} => CloudFront-like = #{tstamp_ymdh}" if retries == 0

              if (!both_tstamps_match)
                retries += 1
                Monitoring::Logging::logger.debug "Instance = #{instance}, Timestamp = #{tstamp} => CloudFront-like = #{tstamp_ymdh}, File basename = #{name}. Retrying(#{retries})"
              end

              break if (both_tstamps_match || retries == max_retries)
            end

            if (!both_tstamps_match)
              Monitoring::Logging::logger.debug "Not equal timestamps CloudFront-like = #{tstamp_ymdh}, Instance = #{instance}, original_tstamp = #{tstamp}, rebuilt_timestamp = #{rebuilt_timestamp}. Delta = #{tstamp_delta}"
              # filepath : i-615f3c85/_var_log_tomcat7_rotated_localhost_access_log.txt1426089661.gz alike
              return [ filepath, RETRY_EXT ]
            elsif (retries > 0)
              Monitoring::Logging::logger.debug "Timestamps matched on a retried conversion for CloudFront-like = #{tstamp_ymdh}, Instance = #{instance}, original_tstamp = #{tstamp}, rebuilt_timestamp = #{rebuilt_timestamp}. Delta = #{tstamp_delta}"
            end

            [base + '.' + tstamp_ymdh, '.txt' + extn]
          rescue StandardError => e
            Monitoring::Logging::logger.debug "Error on timestamp conversion for Instance = #{instance}, Timestamp = #{tstamp}, File basename = #{name}"
            puts e.inspect
            [name, extn]
          end
        else
          [name, extn]
        end
      end

      # Moves new raw logs to a processing bucket.
      #
      # Parameters:
      # +config+:: the hash of configuration options
      #
      # Returns true if file(s) were staged
      Contract ArgsHash, ConfigHash => Bool
      def self.stage_logs_for_emr(args, config)
        Monitoring::Logging::logger.debug 'Staging raw logs...'

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:aws][:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        # Get S3 locations
        in_bucket_array = config[:aws][:s3][:buckets][:raw][:in]
        in_locations = in_bucket_array.map {|name| Sluice::Storage::S3::Location.new(name)}
        processing_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:raw][:processing])
        retry_location = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:raw][:retry])

        # Check whether our processing directory is empty
        unless Sluice::Storage::S3::is_empty?(s3, processing_location)
          raise DirectoryNotEmptyError, "Should not stage files for enrichment, processing bucket #{processing_location} is not empty"
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
            '.+' # this will include those for ndjson/UrbanAirship
          end
        when args[:start].nil?
          Sluice::Storage::files_up_to(args[:end], CF_DATE_FORMAT, CF_FILE_EXT)
        when args[:end].nil?
          Sluice::Storage::files_from(args[:start], CF_DATE_FORMAT, CF_FILE_EXT)
        else
          Sluice::Storage::files_between(args[:start], args[:end], CF_DATE_FORMAT, CF_FILE_EXT)
        end

        fix_filenames = build_fix_filenames(config[:aws][:s3][:region], config[:collectors][:format])
        files_moved = in_locations.map { |in_location|
          Sluice::Storage::S3::move_files(s3, in_location, processing_location, files_to_move, fix_filenames, true)
          Sluice::Storage::S3::move_files(s3, retry_location,  in_location)
        }

        if files_moved.flatten.length == 0
          false
        else
          # Wait for s3 to eventually become consistent
          Monitoring::Logging::logger.debug "Waiting a minute to allow S3 to settle (eventual consistency)"
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
        Monitoring::Logging::logger.debug 'Archiving CloudFront logs...'

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

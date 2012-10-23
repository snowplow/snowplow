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

require 'fog'
require 'thread'

# Ruby module to support the S3-related actions required by
# the Hive-based ETL process.
module S3Tasks

  # Possible errors
  class DirectoryNotEmptyError < StandardError; end
  class UnsupportedFileOperationError < StandardError; end

  # To handle negative file matching
  NegativeRegex = Struct.new(:regex)  

  # Class to describe an S3 location
  class S3Location
    attr_reader :bucket, :dir, :s3location

    # Parameters:
    # +s3location+:: the s3 location config string e.g. "bucket/directory"
    def initialize(s3_location)
      @s3_location = s3_location

      s3_location_match = s3_location.match('^s3n?://([^/]+)/?(.*)/$')
      raise ArgumentError, 'Bad s3 location %s' % s3_location unless s3_location_match

      @bucket = s3_location_match[1]
      @dir = s3_location_match[2]
    end

    def dir_as_path
      if @dir.length > 0
        return @dir+'/'
      else
        return ''
      end
    end

    def to_s
      @s3_location
    end
  end

  # Moves new CloudFront logs to a processing bucket.
  #
  # Parameters:
  # +config+:: the hash of configuration options
  def stage_logs_for_emr(config)
    puts 'Staging CloudFront logs...'

    s3 = new_s3_from(config)

    # get s3 locations
    in_location = S3Location.new(config[:s3][:buckets][:in]);
    processing_location = S3Location.new(config[:s3][:buckets][:processing]);

    # check whether our processing directory is empty
    if s3.directories.get(processing_location.bucket, :prefix => processing_location.dir).files().length > 1
      raise DirectoryNotEmptyError, "The processing directory is not empty"
    end

    # Move the files we need to move (within the date span)
    files_to_move = case
    when (config[:start].nil? and config[:end].nil?)
      '.+'
    when config[:start].nil?
      files_up_to(config[:end])
    when config[:end].nil?
      files_from(config[:start])
    else
      files_between(config[:start], config[:end])
    end

    puts ">>>>>> DEBUG, files to move are:"
    puts files_to_move

    move_files(s3, in_location, processing_location, files_to_move)

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

    s3 = new_s3_from(config)

    # Get s3 locations
    processing_location = S3Location.new(config[:s3][:buckets][:processing]);
    archive_location = S3Location.new(config[:s3][:buckets][:archive]);

    add_date_path = lambda { |filepath|
      if m = filepath.match('[^/]+\.(\d\d\d\d-\d\d-\d\d)-\d\d\.[^/]+\.gz$')
        filename = m[0]
        date = m[1]
        return date + '/' + filename
      else
        return filename
      end
    }

    # Move all the files in the Processing Bucket
    move_files(s3, processing_location, archive_location, '.+', add_date_path)

  end
  module_function :archive_logs

  # Find files within the given date range
  # (inclusive).
  #
  # Parameters:
  # +start_date+:: start date
  # +end_date+:: end date
  def files_between(start_date, end_date)

    dates = []
    Date.parse(start_date).upto(Date.parse(end_date)) do |day|
      dates << day.strftime('%Y-%m-%d')
    end

    '(' + dates.join('|') + ')[^/]+\.gz$'
  end
  module_function :files_between

  # Find files up to (and including) the given date.
  #
  # Returns a regex in a NegativeRegex so that the
  # matcher can negate the match.
  #
  # Parameters:
  # +end_date+:: end date
  def files_up_to(end_date)

    # Let's create a black list from the day
    # after the end_date up to today
    day_after = Date.parse(end_date) + 1
    today = Date.today

    dates = []
    day_after.upto(today) do |day|
      dates << day.strftime('%Y-%m-%d') # Black list
    end

    NegativeRegex.new('(' + dates.join('|') + ')[^/]+\.gz$')
  end
  module_function :files_up_to

  # Find files starting from the given date.
  #
  # Parameters:
  # +start_date+:: start date
  def files_from(start_date)

    # Let's create a white list from the start_date to today
    today = Date.today

    dates = []
    Date.parse(start_date).upto(today) do |day|
      dates << day.strftime('%Y-%m-%d')
    end

    '(' + dates.join('|') + ')[^/]+\.gz$'
  end
  module_function :files_from

  # Helper function to instantiate a new Fog::Storage
  # for S3 based on our config options
  #
  # Parameters:
  # +config+:: the hash of configuration options
  def new_s3_from(config)
    Fog::Storage.new({
      :provider => 'AWS',
      :region => config[:s3][:region],
      :aws_access_key_id => config[:aws][:access_key_id],
      :aws_secret_access_key => config[:aws][:secret_access_key]
    })
  end
  module_function :new_s3_from

  # Delete files from S3 locations concurrently
  #
  # Parameters:
  # +s3+:: A Fog::Storage s3 connection
  # +from+:: S3Location to delete files from
  # +match_regex+:: a regex string to match the files to delete
  def delete_files(s3, from_location, match_regex='.+')

    puts "  deleting files from #{from_location}"
    process_files(:delete, s3, from_location, match_regex)
  end
  module_function :delete_files

  # Copies files between S3 locations concurrently
  #
  # Parameters:
  # +s3+:: A Fog::Storage s3 connection
  # +from+:: S3Location to copy files from
  # +to+:: S3Location to copy files to
  # +match_regex+:: a regex string to match the files to copy
  # +alter_filename_lambda+:: lambda to alter the written filename
  def copy_files(s3, from_location, to_location, match_regex='.+', alter_filename_lambda=false)

    puts "  copying files from #{from_location} to #{to_location}"
    process_files(:copy, s3, from_location, match_regex, to_location, alter_filename_lambda)
  end
  module_function :copy_files

  # Moves files between S3 locations concurrently
  #
  # Parameters:
  # +s3+:: A Fog::Storage s3 connection
  # +from+:: S3Location to move files from
  # +to+:: S3Location to move files to
  # +match_regex+:: a regex string to match the files to move
  # +alter_filename_lambda+:: lambda to alter the written filename
  def move_files(s3, from_location, to_location, match_regex='.+', alter_filename_lambda=false)

    puts "  moving files from #{from_location} to #{to_location}"
    process_files(:move, s3, from_location, match_regex, to_location, alter_filename_lambda)
  end
  module_function :move_files

  # Concurrent file operations between S3 locations. Supports:
  # - Copy
  # - Delete
  # - Move (= Copy + Delete)
  #
  # Parameters:
  # +operation+:: Operation to perform. :copy, :delete, :move supported
  # +s3+:: A Fog::Storage s3 connection
  # +from+:: S3Location to process files from
  # +match_regex+:: a regex string to match the files to process
  # +to+:: S3Location to process files to
  # +alter_filename_lambda+:: lambda to alter the written filename
  def process_files(operation, s3, from_location, match_regex='.+', to_location=nil, alter_filename_lambda=false)

    # Validate that the file operation makes sense
    case operation
    when :copy, :move
      if to_location.nil?
        raise UnsupportedFileOperationError "File operation %s requires a to_location to be set" % operation
      end
    when :delete
      unless to_location.nil?
        raise UnsupportedFileOperationError "File operation %s does not support the to_location argument" % operation
      end
      if alter_filename_lambda.class == Proc
        raise UnsupportedFileOperationError "File operation %s does not support the alter_filename_lambda argument" % operation
      end
    else
      raise UnsupportedFileOperationError "File operation %s is unsupported. Try :copy, :delete or :move" % operation
    end

    files_to_process = []
    threads = []
    mutex = Mutex.new
    complete = false
    marker_opts = {}
    s3_retries = 3

    # if an exception is thrown in a thread that isn't handled, die quickly
    Thread.abort_on_exception = true

    # create ruby threads to concurrently execute s3 operations
    for i in (0...10)

      # each thread pops a file off the files_to_process array, and moves it.
      # We loop until there are no more files
      threads << Thread.new do
        loop do
          file = false
          match = false

          # Critical section
          # only allow one thread to modify the array at any time
          mutex.synchronize do

            while !complete && !match do
              if files_to_process.size == 0
                # s3 batches 1000 files per request
                # we load up our array with the files to move
                #puts "-- loading more results"
                files_to_process = s3.directories.get(from_location.bucket, :prefix => from_location.dir).files.all(marker_opts)
                #puts "-- got #{files_to_process.size} results"
                # if we don't have any files after the s3 request, we're complete
                if files_to_process.size == 0
                  complete = true
                  next
                else
                  marker_opts['marker'] = files_to_process.last.key

                  # By reversing the array we can use pop and get FIFO behaviour
                  # instead of the performance penalty incurred by unshift
                  files_to_process = files_to_process.reverse
                end
              end

              file = files_to_process.pop
              match = if match_regex.is_a? NegativeRegex
                        !file.key.match(match_regex.regex)
                      else
                        file.key.match(match_regex)
                      end
            end
          end

          # if we don't have a match, then we must be complete
          break unless match # exit the thread

          # match the filename, ignoring directory
          file_match = file.key.match('([^/]+)$')

          if alter_filename_lambda.class == Proc
            filename = alter_filename_lambda.call(file_match[1])
          else
            filename = file_match[1]
          end

          # What are we doing?
          case operation
          when :move
            puts "    MOVE #{from_location.bucket}/#{file.key} -> #{to_location.bucket}/#{to_location.dir_as_path}#{filename}"            
          when :copy
            puts "    COPY #{from_location.bucket}/#{file.key} +-> #{to_location.bucket}/#{to_location.dir_as_path}#{filename}"  
          when :delete
            puts "    DELETE x #{from_location.bucket}/#{file.key}" 
          end

          # A move or copy starts with a copy file
          if [:move, :copy].include? operation
            i = 0
            begin
              file.copy(to_location.bucket, to_location.dir_as_path + filename)
              puts "      +-> #{to_location.bucket}/#{to_location.dir_as_path}#{filename}"
            rescue
              raise unless i < s3_retries
              puts "Problem copying #{file.key}. Retrying.", $!, $@
              sleep(10)  # give us a bit of time before retrying
              i += 1
              retry
            end
          end

          # A move or delete ends with a delete
          if [:move, :delete].include? operation
            i = 0
            begin
              file.destroy()
              puts "      x #{from_location.bucket}/#{file.key}"
            rescue
              raise unless i < s3_retries
              puts "Problem destroying #{file.key}. Retrying.", $!, $@
              sleep(10) # give us a bit of time before retrying
              i += 1
              retry
            end
          end
        end
      end
    end

    # wait for threads to finish
    threads.each { |aThread|  aThread.join }

  end
  module_function :process_files

end
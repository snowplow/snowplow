# Copyright (c) 2012-2013 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2013 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'plissken'

# Ruby module to support the load of Snowplow events into Redshift
module SnowPlow
  module StorageLoader
    module RedshiftLoader

      # Constants for the load process
      EVENT_FIELD_SEPARATOR = "\\t"

      # Loads the Snowplow event files into Redshift.
      #
      # Parameters:
      # +config+:: the configuration options
      # +target+:: the configuration for this specific target
      def load_events(config, target)
        puts "Loading Snowplow events into #{target[:name]} (Redshift cluster)..."

        # Assemble the relevant parameters for the bulk load query
        credentials = get_credentials(config)
        comprows = if config[:include].include?('compudate')
                     "COMPUPDATE COMPROWS #{config[:comprows]}"
                   else
                     ""
                   end

        # Build the Array of queries we will run
        queries = [
          "COPY #{target[:table]} FROM '#{config[:s3][:buckets][:in]}' CREDENTIALS '#{credentials}' DELIMITER '#{EVENT_FIELD_SEPARATOR}' MAXERROR #{target[:maxerror]} EMPTYASNULL FILLRECORD TRUNCATECOLUMNS #{comprows} TIMEFORMAT 'auto' ACCEPTINVCHARS;",
        ]
        unless config[:skip].include?('analyze')
          queries << "ANALYZE #{target[:table]};"
        end
        if config[:include].include?('vacuum')
          queries << "VACUUM SORT ONLY #{target[:table]};"
        end
        unless config[:skip].include?('shred')
          queries << load_shredded_types(config, types)
        end

        status = PostgresLoader.execute_transaction(target, queries)
        unless status == []
          raise DatabaseLoadError, "#{status[1]} error executing #{status[0]}: #{status[2]}"
        end
      end
      module_function :load_events

    private

      # Generates a COPY FROM JSON for each shredded JSON
      #
      # Parameters:
      # +config+:: the configuration options
      # +target+:: the configuration for this specific target
      def load_shredded_types(config, target)
        []
      end
      module_function :load_shredded_types

      # Derives the table name in Redshift from the Iglu
      # schema key.
      #
      # Should convert:
      #   org.schema/WebPage/jsonschema/1
      # to:
      #   org_schema_web_page_1
      #
      # Parameters:
      # +partial_key+:: the Iglu schema key to determine the
      #                 table name for. Partial because the
      #                 verison contains only the MODEL (not
      #                 the whole SchemaVer).
      def partial_key_as_table(partial_key)
        partial_key_regexp = /
            ^
            (?<vendor>.+)
            \/
            (?<name>.+)
            \/
            (?<format>.+)
            \/
            (?<model>.+)
            $
        /x
        parts = partial_key_regexp.match(partial_key)

        # Replace any periods in vendor or name with underscore
        # Any camelCase or PascalCase to snake_case
        fix = lambda { |value|
          Hash.new.send(:underscore, value).tr('.', '_')
        }
        vendor = fix.call(parts[:vendor])
        name   = fix.call(parts[:name])
        model  = parts[:model]

        "#{vendor}_#{name}_#{model}"
      end
      module_function :partial_key_as_table

      # Constructs the credentials expression for a
      # Redshift COPY statement.
      #
      # Parameters:
      # +config+:: the configuration options
      def get_credentials(config)
        "aws_access_key_id=#{config[:aws][:access_key_id]};aws_secret_access_key=#{config[:aws][:secret_access_key]}"
      end
      module_function :get_credentials

      # Creates an S3 objectpath for the COPY FROM JSON.
      # Note that Redshift COPY treats the S3 objectpath
      # as a prefixed path - i.e. you get "file globbing"
      # for free by specifying a partial folder path.
      #
      # Parameters:
      # +shredded_path+:: the S3 path to the shredded types
      # +run_id+:: the folder for this run, e.g.
      #            run=2014-06-24-08-19-52
      # +partial_key+:: the Iglu schema key to determine the
      #                 table name for. Partial because the
      #                 verison contains only the MODEL (not
      #                 the whole SchemaVer).
      def get_s3_objectpath(shredded_path, run_id, partial_key)
        # Trailing hyphen ensures we don't accidentally load
        # 11-x-x versions into a 1-x-x table
        "#{shredded_path}#{run_id}/#{partial_key}-"
      end
      module_function :get_s3_objectpath

      # Constructs a COPY FROM JSON of a shredded JSON
      # into a dedicated table
      #
      # Parameters:
      # +config+:: the configuration options
      # +run_id+:: the folder for this run, e.g.
      #            run=2014-06-24-08-19-52
      # +partial_key+:: the Iglu schema key to determine the
      #                 table name for. Partial because the
      #                 verison contains only the MODEL (not
      #                 the whole SchemaVer)
      # +jsonpaths_file+:: the file on S3 containing the JSON Path
      #                    statements to load the JSON
      def build_copy_for_shredded_type(config, run_id, partial_key, jsonpaths_file)

        schema = "TODO"
        table = partial_key_as_table(partial_key)
        objectpath = get_s3_objectpath(config[:s3][:buckets][:shredded][:good], run_id, partial_key)
        credentials = get_credentials(config)

        "COPY #{schema}#{table} FROM '#{objectpath}' CREDENTIALS '#{credentials}' JSON '#{jsonpaths_file}';"
      end
      module_function :build_copy_for_shredded_type

    end
  end
end

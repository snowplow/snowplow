# Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
require 'contracts'

# Ruby module to support the load of Snowplow events into Redshift
module Snowplow
  module StorageLoader
    module RedshiftLoader

      include Contracts

      # Constants for the load process
      EVENT_FIELD_SEPARATOR = "\\t"

      # Used to find the altered enriched events
      ALTERED_ENRICHED_PATTERN = /(run=[0-9\-]+\/atomic-events)/

      # Versions 0.5.0 and earlier of Hadoop Shred don't copy atomic.events into the shredded bucket
      OLD_ENRICHED_PATTERN = /0\.[0-5]\.[0-9](-rc[0-9]+|)/

      # Versions 0.11.0 and earlier of the rdb shredder didn't use the field names when writing
      # shredded types
      OLD_SHRED_PATTERN = /0\.([0-9]|1[0-1])\.[0-9](-rc[0-9]+|)/

      SqlStatements = Struct.new(:copy, :analyze, :vacuum)

      # Loads the Snowplow event files and shredded type
      # files into Redshift.
      #
      # Parameters:
      # +config+:: the configuration options
      # +target+:: the configuration for this specific target
      # +snowplow_tracking_enabled+:: whether we should emit Snowplow events for this
      Contract Hash, Hash, Bool => nil
      def self.load_events_and_shredded_types(config, target, snowplow_tracking_enabled)
        puts "Loading Snowplow events and shredded types into #{target[:name]} (Redshift cluster)..."

        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:aws][:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        schema = target[:schema]
        events_table = schema + '.events'

        # First let's get our statements for shredding (if any)
        shredded_statements = if OLD_SHRED_PATTERN.match(config[:storage][:versions][:rdb_shredder])
          get_shredded_statements(config, target, s3, true)
        else
          get_shredded_statements(config, target, s3)
        end

        # Now let's get the manifest statement
        manifest_statement = get_manifest_statement(schema, shredded_statements.length)

        # Build our main transaction, consisting of COPY and COPY FROM JSON
        # statements, and potentially also a set of table ANALYZE statements.

        atomic_events_location = if OLD_ENRICHED_PATTERN.match(config[:storage][:versions][:rdb_shredder])
          :enriched
        else
          :shredded
        end

        copy_statements = if atomic_events_location == :shredded
          loc = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:shredded][:good])
          altered_enriched_filepath = Sluice::Storage::S3::list_files(s3, loc).find { |file|
            ALTERED_ENRICHED_PATTERN.match(file.key)
          }
          if altered_enriched_filepath.nil?
            raise DatabaseLoadError, 'Cannot find atomic-events directory in shredded/good'
          end
          # Of the form "run=xxx/atomic-events"
          altered_enriched_subdirectory = ALTERED_ENRICHED_PATTERN.match(altered_enriched_filepath.key)[1]
          [build_copy_from_tsv_statement(config, config[:aws][:s3][:buckets][:shredded][:good] + altered_enriched_subdirectory, events_table, target[:maxError].to_i)]
        else
          [build_copy_from_tsv_statement(config, config[:aws][:s3][:buckets][:enriched][:good], events_table, target[:maxError].to_i)]
        end + shredded_statements.map(&:copy) + [manifest_statement]

        credentials = [config[:aws][:access_key_id], config[:aws][:secret_access_key]]

        status = PostgresLoader.execute_transaction(target, copy_statements)
        unless status == []
          raw_error_message = "#{status[1]} error executing COPY statements: #{status[0]}: #{status[2]}"
          error_message = Sanitization.sanitize_message(raw_error_message, credentials)
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_load_failed(error_message)
          end
          raise DatabaseLoadError, error_message
        end

        if snowplow_tracking_enabled
          Monitoring::Snowplow.instance.track_load_succeeded()
        end

        # If vacuum is requested, build a set of VACUUM statements
        # and execute them in series. VACUUMs cannot be performed
        # inside of a transaction
        if config[:include].include?('vacuum')
          vacuum_statements = [build_vacuum_statement(events_table)] + shredded_statements.map(&:vacuum).uniq
          vacuum_status = PostgresLoader.execute_queries(target, vacuum_statements)
          unless vacuum_status == []
            raise DatabaseLoadError, Sanitization.sanitize_message("#{vacuum_status[1]} error executing VACUUM statements: #{vacuum_status[0]}: #{vacuum_status[2]}", credentials)
          end
        end

        # ANALYZE statements should be executed after VACUUM statements.
        unless config[:skip].include?('analyze')
          analyze_statements = [build_analyze_statement(events_table)] + shredded_statements.map(&:analyze).uniq
          analyze_status = PostgresLoader.execute_transaction(target, analyze_statements)
          unless analyze_status == []
            raise DatabaseLoadError, Sanitization.sanitize_message("#{analyze_status[1]} error executing ANALYZE statements: #{analyze_status[0]}: #{analyze_status[2]}", credentials)
          end
        end

        nil
      end

    private

      # Generates an array of SQL statements for loading
      # the shredded types.
      #
      # Parameters:
      # +config+:: the configuration options
      # +target+:: the configuration for this specific target
      # +s3+::     the Fog object for accessing S3
      # +legacy+:: Whether we're using the old hadoop shred
      Contract Hash, Hash, FogStorage, Bool => ArrayOf[SqlStatements]
      def self.get_shredded_statements(config, target, s3, legacy=false)

        if config[:skip].include?('shred') # No shredded types to load
          []
        else
          schema = target[:schema]

          ShreddedType.discover_shredded_types(s3, config[:aws][:s3][:buckets][:shredded][:good], schema, legacy).map { |st|

            jsonpaths_file = st.discover_jsonpaths_file(s3, config[:aws][:s3][:buckets][:jsonpath_assets])
            if jsonpaths_file.nil?
              raise DatabaseLoadError, "Cannot find JSON Paths file to load #{st.s3_objectpath} into #{st.table}"
            end

            SqlStatements.new(
              build_copy_from_json_statement(config, st.s3_objectpath, jsonpaths_file, st.table, target[:maxError].to_i),
              build_analyze_statement(st.table),
              build_vacuum_statement(st.table)
            )
          }
        end
      end

      # Generates the SQL statement for updating the
      # manifest table
      #
      # Parameters:
      # +schema+:: the name of the events schema being loaded
      # +shredded_cardinality+:: the number of shredded child events and contexts tables loaded in this run
      Contract String, Num => String
      def self.get_manifest_statement(schema, shredded_cardinality)

        schema = if schema.empty? then "" else "#{schema}." end
        events_table = schema + "events"

        "INSERT INTO #{schema}manifest
          SELECT etl_tstamp, sysdate AS commit_tstamp, count(*) AS event_count, #{shredded_cardinality} AS shredded_cardinality
          FROM #{events_table}
          WHERE etl_tstamp IS NOT null
          GROUP BY 1
          ORDER BY etl_tstamp DESC
          LIMIT 1;
        "
      end

      # Replaces an initial "s3n" with "s3" in an S3 path
      Contract String => String
      def self.fix_s3_path(path)
        path.gsub(/^s3n/, 's3')
      end

      # Constructs the COPY statement to load the enriched
      # event TSV files into Redshift.
      #
      # Parameters:
      # +config+:: the configuration options
      # +s3_object_path+:: the S3 path to the files containing
      #                    this shredded type
      # +table+:: the name of the table to load, including
      #           optional schema
      # +maxerror+:: how many errors to allow for this COPY
      Contract Hash, String, String, Num => String
      def self.build_copy_from_tsv_statement(config, s3_objectpath, table, maxerror)

        # Assemble the relevant parameters for the bulk load query
        credentials = get_credentials(config)
        compression_format = get_compression_format(config[:enrich][:output_compression])
        fixed_objectpath = fix_s3_path(s3_objectpath)
        comprows =
          if config[:include].include?('compudate')
            "COMPUPDATE COMPROWS #{config[:comprows]}"
          else
            ""
          end

        "COPY #{table} FROM '#{fixed_objectpath}' CREDENTIALS '#{credentials}' REGION AS '#{config[:aws][:s3][:region]}' DELIMITER '#{EVENT_FIELD_SEPARATOR}' MAXERROR #{maxerror} EMPTYASNULL FILLRECORD TRUNCATECOLUMNS #{comprows} TIMEFORMAT 'auto' ACCEPTINVCHARS #{compression_format};"
      end

      # Constructs the COPY FROM JSON statement required for
      # loading a shredded JSON into a dedicated table; also
      # returns the table name.
      #
      # Parameters:
      # +config+:: the configuration options
      # +s3_object_path+:: the S3 path to the files containing
      #                    this shredded type
      # +jsonpaths_file+:: the file on S3 containing the JSON Path
      #                    statements to load the JSON
      # +table+:: the name of the table to load, including
      #           optional schema
      # +maxerror+:: how many errors to allow for this COPY
      Contract Hash, String, String, String, Num => String
      def self.build_copy_from_json_statement(config, s3_objectpath, jsonpaths_file, table, maxerror)
        credentials = get_credentials(config)
        compression_format = get_compression_format(config[:enrich][:output_compression])
        fixed_objectpath = fix_s3_path(s3_objectpath)
        # TODO: what about COMPUPDATE/ROWS?
        "COPY #{table} FROM '#{fixed_objectpath}' CREDENTIALS '#{credentials}' JSON AS '#{jsonpaths_file}' REGION AS '#{config[:aws][:s3][:region]}' MAXERROR #{maxerror} TRUNCATECOLUMNS TIMEFORMAT 'auto' ACCEPTINVCHARS #{compression_format};"
      end

      # Builds an ANALYZE statement for the
      # given table.
      #
      # Parameters:
      # +table+:: the name of the table to analyze
      Contract String => String
      def self.build_analyze_statement(table)
        "ANALYZE #{table};"
      end

      # Builds a VACUUM statement for the
      # given table.
      #
      # Parameters:
      # +table+:: the name of the table to analyze
      Contract String => String
      def self.build_vacuum_statement(table)
        "VACUUM SORT ONLY #{table};"
      end

      # Constructs the credentials expression for a
      # Redshift COPY statement.
      #
      # Parameters:
      # +config+:: the configuration options
      Contract Hash => String
      def self.get_credentials(config)
        "aws_access_key_id=#{config[:aws][:access_key_id]};aws_secret_access_key=#{config[:aws][:secret_access_key]}"
      end

      # Returns the compression format for a
      # Redshift COPY statement.
      #
      # Parameters:
      # +output_codec+:: the output code
      Contract String => String
      def self.get_compression_format(output_codec)
        if output_codec == 'NONE'
          ''
        elsif output_codec == 'GZIP'
          'GZIP'
        end
        # TODO: fix non-exhaustive match above
      end

    end
  end
end

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

require 'contracts'
include Contracts

# Ruby module to support the load of Snowplow events into Redshift
module Snowplow
  module StorageLoader
    module RedshiftLoader

      # Constants for the load process
      EVENT_FIELD_SEPARATOR = "\\t"

      SqlStatements = Struct.new(:copy, :analyze, :vacuum)

      # Loads the Snowplow event files and shredded type
      # files into Redshift.
      #
      # Parameters:
      # +config+:: the configuration options
      # +target+:: the configuration for this specific target
      Contract Hash, Hash => nil
      def load_events_and_shredded_types(config, target)
        puts "Loading Snowplow events and shredded types into #{target[:name]} (Redshift cluster)..."

        # First let's get our statements for shredding (if any)
        shredded_statements = get_shredded_statements(config, target)

        # Build our main transaction, consisting of COPY and COPY FROM JSON
        # statements, and potentially also a set of table ANALYZE statements.
        copy_analyze_statements = [
          build_copy_from_tsv_statement(config, config[:s3][:buckets][:enriched][:good], target[:table], target[:maxerror])
        ]
        copy_analyze_statements.push(*shredded_statements.map(&:copy))

        unless config[:skip].include?('analyze')
          copy_analyze_statements << build_analyze_statement(target[:table])
          copy_analyze_statements.push(*shredded_statements.map(&:analyze).uniq)
        end

        status = PostgresLoader.execute_transaction(target, copy_analyze_statements)
        unless status == []
          raise DatabaseLoadError, "#{status[1]} error executing COPY and ANALYZE statements: #{status[0]}: #{status[2]}"
        end

        # If vacuum is requested, build a set of VACUUM statements
        # and execute them in series. VACUUMs cannot be performed
        # inside of a transaction
        if config[:include].include?('vacuum')
          vacuum_statements = [
            build_vacuum_statement(target[:table])
          ]
          vacuum_statements.push(*shredded_statements.map(&:vacuum).uniq)

          status = PostgresLoader.execute_queries(target, vacuum_statements)
          unless status == []
            raise DatabaseLoadError, "#{status[1]} error executing VACUUM statements: #{status[0]}: #{status[2]}"
          end      
        end

        nil
      end
      module_function :load_events_and_shredded_types

    private

      # Generates an array of SQL statements for loading
      # the shredded types.
      #
      # Parameters:
      # +config+:: the configuration options
      # +target+:: the configuration for this specific target
      Contract Hash, Hash => ArrayOf[SqlStatements]
      def get_shredded_statements(config, target)

        if config[:skip].include?('shred') # No shredded types to load
          []
        else
          s3 = Sluice::Storage::S3::new_fog_s3_from(
            config[:s3][:region],
            config[:aws][:access_key_id],
            config[:aws][:secret_access_key])
          schema = extract_schema(target[:table])

          ShreddedType.discover_shredded_types(s3, config[:s3][:buckets][:shredded][:good], schema).map { |st|

            jsonpaths_file = st.discover_jsonpaths_file(s3, config[:s3][:buckets][:jsonpath_assets])
            if jsonpaths_file.nil?
              raise DatabaseLoadError, "Cannot find JSON Paths file to load #{st.s3_objectpath} into #{st.table}"
            end

            SqlStatements.new(
              build_copy_from_json_statement(config, st.s3_objectpath, jsonpaths_file, st.table, target[:maxerror]),
              build_analyze_statement(st.table),
              build_vacuum_statement(st.table)
            )
          }
        end
      end
      module_function :get_shredded_statements

      # Looks at the events table to determine if there's
      # a schema we should use for the shredded type tables.
      #
      # Parameters:
      # +events_table+:: the events table to load into
      Contract String => Maybe[String]
      def extract_schema(events_table)
        parts = events_table.split(/\./)
        if parts.size > 1 then parts[0] else nil end
      end
      module_function :extract_schema

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
      def build_copy_from_tsv_statement(config, s3_objectpath, table, maxerror)

        # Assemble the relevant parameters for the bulk load query
        credentials = get_credentials(config)
        comprows =
          if config[:include].include?('compudate')
            "COMPUPDATE COMPROWS #{config[:comprows]}"
          else
            ""
          end

        "COPY #{table} FROM '#{s3_objectpath}' CREDENTIALS '#{credentials}' REGION AS '#{config[:s3][:region]}' DELIMITER '#{EVENT_FIELD_SEPARATOR}' MAXERROR #{maxerror} EMPTYASNULL FILLRECORD TRUNCATECOLUMNS #{comprows} TIMEFORMAT 'auto' ACCEPTINVCHARS;"
      end
      module_function :build_copy_from_tsv_statement

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
      def build_copy_from_json_statement(config, s3_objectpath, jsonpaths_file, table, maxerror)
        credentials = get_credentials(config)
        # TODO: what about COMPUPDATE/ROWS?
        "COPY #{table} FROM '#{s3_objectpath}' CREDENTIALS '#{credentials}' JSON AS '#{jsonpaths_file}' REGION AS '#{config[:s3][:region]}' MAXERROR #{maxerror} TRUNCATECOLUMNS TIMEFORMAT 'auto' ACCEPTINVCHARS;"
      end
      module_function :build_copy_from_json_statement

      # Builds an ANALYZE statement for the
      # given table.
      #
      # Parameters:
      # +table+:: the name of the table to analyze
      Contract Hash => String
      def build_analyze_statement(table)
        "ANALYZE #{table};"
      end
      module_function :build_analyze_statement

      # Builds a VACUUM statement for the
      # given table.
      #
      # Parameters:
      # +table+:: the name of the table to analyze
      Contract Hash => String
      def build_vacuum_statement(table)
        "VACUUM SORT ONLY #{table};"
      end
      module_function :build_vacuum_statement

      # Constructs the credentials expression for a
      # Redshift COPY statement.
      #
      # Parameters:
      # +config+:: the configuration options
      Contract Hash => String
      def get_credentials(config)
        "aws_access_key_id=#{config[:aws][:access_key_id]};aws_secret_access_key=#{config[:aws][:secret_access_key]}"
      end
      module_function :get_credentials

    end
  end
end

# Copyright (c) 2013-2017 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2013-2015 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'jdbc/postgres'
require 'contracts'

Jdbc::Postgres.load_driver

# Ruby module to support the load of Snowplow events into PostgreSQL.
module Snowplow
  module StorageLoader
    module PostgresLoader

      include Contracts

      # Constants for the load process
      EVENT_FILES = "part-*"
      EVENT_FIELD_SEPARATOR = "	"
      NULL_STRING = ""
      QUOTE_CHAR = "\\x01"
      ESCAPE_CHAR = "\\x02"

      # Loads the Snowplow event files into Postgres.
      #
      # Parameters:
      # +events_dir+:: the directory holding the event files to load 
      # +target+:: the configuration options for this target
      # +skip_steps+:: Array of steps to skip
      # +include_steps+:: Array of optional steps to include
      def self.load_events(events_dir, target, skip_steps, include_steps, snowplow_tracking_enabled)
        puts "Loading Snowplow events into #{target[:name]} (PostgreSQL database)..."

        event_files = get_event_files(events_dir)
        status = copy_via_stdin(target, event_files)

        unless status == []
          error_message = "#{status[1]} error loading #{status[0]}: #{status[2]}"
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_load_failed(error_message)
          end
          raise DatabaseLoadError, error_message
        end

        if snowplow_tracking_enabled
          Monitoring::Snowplow.instance.track_load_succeeded()
        end

        post_processing = nil
        unless skip_steps.include?('vacuum')
          post_processing = "VACUUM "
        end
        if include_steps.include?('analyze')
          post_processing = "ANALYZE " + (post_processing || "")
        end

        schema = target[:schema]
        events_table = schema + ".events"

        unless post_processing.nil?
          status = execute_queries(target, [ "#{post_processing}#{events_table};" ] )
          unless status == []
            raise DatabaseLoadError, "#{status[1]} error executing #{status[0]}: #{status[2]}"
          end
        end  
      end

      # Adapted from:  https://bitbucket.org/ged/ruby-pg/raw/9812218e0654caa58f8604838bc368434f7b3828/sample/copyfrom.rb
      #
      # Execute a series of copies from stdin by streaming the given files
      # directly into the postgres copy statement.  Stops as soon as an error
      # is encountered.  At that point, it returns a 'tuple' of the error class
      # and message and the file that caused the error.
      #
      # Parameters:
      # +target+:: the configuration options for this target
      # +files+:: the data files to copy into the database
      def self.copy_via_stdin(target, files)
        puts "Opening database connection ..."

        schema = target[:schema]
        events_table = schema + ".events"

        conn = get_connection(target)
        conn.setAutoCommit(false)

        copy_statement = "COPY #{events_table} FROM STDIN WITH CSV ESCAPE E'#{ESCAPE_CHAR}' QUOTE E'#{QUOTE_CHAR}' DELIMITER '#{EVENT_FIELD_SEPARATOR}' NULL '#{NULL_STRING}'"

        status = []

        files.each do |file|
          puts "Running COPY command with data from: #{file}"
          begin
            copy_manager = org.postgresql.copy.CopyManager.new(conn)
            file_reader = java.io.FileReader.new(file)
            copy_manager.copyIn(copy_statement, file_reader)
          rescue Java::JavaSql::SQLException, Java::JavaSql::SQLTimeoutException => err
            status = [file, err.class, err.message]
            break
          end
        end

        if status == []
          conn.commit()
        else
          conn.rollback()
        end

        conn.close()

        return status
      end

      # Converts a set of queries into a
      # single Redshift read-write
      # transaction.
      #
      # Parameters:
      # +target+:: the configuration options for this target
      # +queries+:: the Redshift queries to execute sequentially
      #
      # Returns either an empty list on success, or on failure
      # a list of the form [query, err_class, err_message]      
      def self.execute_transaction(target, queries)

        transaction = (
          [ "BEGIN;" ] +
          
          queries +
          
          [ "COMMIT;" ]
        ).join("\n")

        execute_queries(target, [ transaction ])
      end

      # Execute a chain of SQL commands, stopping as soon as
      # an error is encountered. At that point, it returns a
      # 'tuple' of the error class and message and the command
      # that caused the error
      #
      # Parameters:
      # +target+:: the configuration options for this target
      # +queries+:: the Redshift queries to execute sequentially
      #
      # Returns either an empty list on success, or on failure
      # a list of the form [query, err_class, err_message]
      def self.execute_queries(target, queries)

        conn = get_connection(target)

        status = []
        queries.each do |q|
          begin
            conn.createStatement.executeUpdate(q)
          rescue Java::JavaSql::SQLException, Java::JavaSql::SQLTimeoutException => err
            status = [q, err.class, err.message]
            break
          end
        end

        conn.close()
        return status
      end

      # Get a java.sql.Connection to a database
      def self.get_connection(target)
        connection_url = "jdbc:postgresql://#{target[:host]}:#{target[:port]}/#{target[:database]}"

        props = java.util.Properties.new
        props.set_property :user, target[:username]
        props.set_property :password, target[:password]
        props.set_property :sslmode, fix_sslmode(target[:sslMode])
        props.set_property :tcpKeepAlive, "true" # TODO: make this configurable if any adverse effects

        # Used instead of Java::JavaSql::DriverManager.getConnection to prevent "no suitable driver found" error
        org.postgresql.Driver.new.connect(connection_url, props)
      end

      private

      # Return the list of event files.
      #
      # Parameters:
      # +events_dir+:: the directory holding the event files to load 
      #
      # Returns the array of cold files
      def self.get_event_files(events_dir)

        Dir[File.join(events_dir, '**', 'atomic-events', EVENT_FILES)].select { |f|
          File.file?(f) # In case of a dir ending in .tsv
        }
      end

      Contract String => String
      def self.fix_sslmode(ssl_mode)
        ssl_mode.downcase.tr('_', '-')
      end

    end
  end
end

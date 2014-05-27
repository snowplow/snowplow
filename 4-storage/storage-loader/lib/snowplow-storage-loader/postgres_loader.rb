# Copyright (c) 2013 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2013 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'pg'

require 'csv'

# Ruby module to support the load of Snowplow events into PostgreSQL.
module SnowPlow
  module StorageLoader
    module PostgresLoader

      # Constants for the load process
      EVENT_FILES = "part-*"
      EVENT_FIELD_SEPARATOR = "	"
      NULL_STRING = ""
      QUOTE_CHAR = "\\x01"
      ESCAPE_CHAR = "\\x02"

      # Loads the SnowPlow event files into Postgres.
      #
      # Parameters:
      # +events_dir+:: the directory holding the event files to load 
      # +target+:: the configuration options for this target
      # +skip_steps+:: Array of steps to skip
      # +include_steps+:: Array of optional steps to include
      def load_events(events_dir, target, skip_steps, include_steps)
        puts "Loading Snowplow events into #{target[:name]} (PostgreSQL database)..."

        event_files = get_event_files(events_dir)

        if target[:host] == 'localhost'
          queries = event_files.map { |f|
              "COPY #{target[:table]} FROM '#{f}' WITH CSV ESCAPE E'#{ESCAPE_CHAR}' QUOTE E'#{QUOTE_CHAR}' DELIMITER '#{EVENT_FIELD_SEPARATOR}' NULL '#{NULL_STRING}';"
          }
          
          status = execute_transaction(target, queries)
          unless status == []
            raise DatabaseLoadError, "#{status[1]} error executing #{status[0]}: #{status[2]}"
          end
        else
          status = execute_from_csv(target, event_files)
          unless status == []
            raise DatabaseLoadError, "#{status[1]} error executing #{status[0]}: #{status[2]}"
          end
        end

        post_processing = nil
        unless skip_steps.include?('analyze')
          post_processing = "ANALYZE "
        end
        if include_steps.include?('vacuum')
          post_processing = "VACUUM " + (post_processing || "")
        end

        unless post_processing.nil?
          status = execute_queries(target, [ "#{post_processing}#{target[:table]};" ] )
          unless status == []
            raise DatabaseLoadError, "#{status[1]} error executing #{status[0]}: #{status[2]}"
          end
        end  
      end
      module_function :load_events


      # Parse CSV and execute queries in a prepared statement
      #
      # Parameters:
      # +target+:: the configuration options for this target
      # +csv+:: the CSV file
      #
      # Returns number of queries that were executed.
      def execute_from_csv(target, csv)
        status = []
        conn = connect(target)
        csv_counter = 1
        total = 0
        csv.each do |f|
          n = 0
          prepared = false
          CSV.foreach(csv, options={ :col_sep => EVENT_FIELD_SEPARATOR, :quote_char => "\x01" }) do |row|
            if not prepared
              query = "INSERT INTO #{target[:table]} VALUES ("
              for i in 1..row.length
                query += "$#{i},"
              end
              conn.prepare('insert1', query.chomp(',') + ')')
              prepared = true
            end
            begin
              conn.exec_prepared('insert1', row)
            rescue PG::Error => err
              status = [q, err.class, err.message]
              break
            end
            n += 1
          end
          puts "   Load file #{csv_counter}/#{csv.length} completed, #{n} records loaded successfully."
          csv_counter += 1
          total += n
          if status != []
            break
          end
        end

        conn.finish

        puts "Load completed, #{total} records."
        return status
      end
      module_function :execute_from_csv

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
      def execute_transaction(target, queries)

        transaction = (
          [ "BEGIN;" ] +
          
          queries +
          
          [ "COMMIT;" ]
        ).join("\n")

        execute_queries(target, [ transaction ])
      end
      module_function :execute_transaction

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
      def execute_queries(target, queries)

        conn = connect(target)

        status = []
        queries.each do |q|
          begin
            conn.exec("#{q}")
          rescue PG::Error => err
            status = [q, err.class, err.message]
            break
          end
        end

        conn.finish
        return status
      end
      module_function :execute_queries

      private

      # Return the list of event files.
      #
      # Parameters:
      # +events_dir+:: the directory holding the event files to load 
      #
      # Returns the array of cold files
      def get_event_files(events_dir)

        Dir[File.join(events_dir, '**', EVENT_FILES)].select { |f|
          File.file?(f) # In case of a dir ending in .tsv
        }
      end
      module_function :get_event_files


      # Connect to PostgreSQL database
      #
      # Parameters:
      # +target+:: the configuration options for this target
      #
      # Return a PGconn object
      def connect(target)
        conn = PG.connect({:host     => target[:host],
                           :dbname   => target[:database],
                           :port     => target[:port],
                           :user     => target[:username],
                           :password => target[:password]
                          })
        return conn
      end
      module_function :connect

    end
  end
end

# vim: set tabstop=2 shiftwidth=2 expandtab :

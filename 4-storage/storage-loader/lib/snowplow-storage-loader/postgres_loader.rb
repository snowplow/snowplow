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
        event_files.each do |f|
          copy_via_stdin(
            target,
            "COPY #{target[:table]} FROM STDIN WITH CSV ESCAPE E'#{ESCAPE_CHAR}' QUOTE E'#{QUOTE_CHAR}' DELIMITER '#{EVENT_FIELD_SEPARATOR}' NULL '#{NULL_STRING}'",
            f
          )
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

      COPY_BUFFER_SIZE = 1024

      # Adapted from:  https://bitbucket.org/ged/ruby-pg/raw/9812218e0654caa58f8604838bc368434f7b3828/sample/copyfrom.rb
      # Streams a file from disk directly into the postgres copy statement
      # FIXME: refactor COPY into this method
      # FIXME: wrap transaction around sequential COPYs
      # FIXME: battletest; how does this behave when it fails?
      def copy_via_stdin(target, copy_statement, file)
        puts "Opening database connection ..."
        conn = PG.connect({:host     => target[:host],
                           :dbname   => target[:database],
                           :port     => target[:port],
                           :user     => target[:username],
                           :password => target[:password]
                          })

        puts "Running COPY command with data ..."
        buf = ''
        conn.transaction do
          conn.exec(copy_statement)
          begin
            File.open(file, 'r+') do |copy_data|
              while copy_data.read( COPY_BUFFER_SIZE, buf )
                until conn.put_copy_data( buf )
                  puts "  waiting for connection to be writable..."
                  sleep 0.1
                end
              end
            end
          rescue Errno => err
            errmsg = "%s while reading copy data: %s" % [ err.class.name, err.message ]
            conn.put_copy_end( errmsg )
          else
            conn.put_copy_end
            while res = conn.get_result
              puts "Result of COPY is: %s" % [ res.res_status(res.result_status) ]
            end
          end
        end

        conn.finish
      end
      module_function :copy_via_stdin

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

        conn = PG.connect({:host     => target[:host],
                           :dbname   => target[:database],
                           :port     => target[:port],
                           :user     => target[:username],
                           :password => target[:password]
                          })

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

    end
  end
end

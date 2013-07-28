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
      EVENT_FIELD_SEPARATOR = "\\t"

      # Loads the SnowPlow event files into Postgres.
      #
      # Parameters:
      # +config+:: the hash of configuration options 
      def load_events(config)
        puts "Loading Snowplow events into PostgreSQL..."

        empty_as_null = "EMPTYASNULL" if config[:storage][:empty_as_null]
        queries = ["COPY #{config[:storage][:table]} FROM '#{config[:download][:folder]}' CREDENTIALS '#{credentials}' DELIMITER '#{EVENT_FIELD_SEPARATOR}' MAXERROR #{config[:storage][:max_error]} #{empty_as_null}",
                   "VACUUM FULL ANALYZE #{config[:storage][:table]}"]

        status = execute_queries(config, queries)
        unless status == []
          raise DatabaseLoadError, "#{status[1]} error executing #{status[0]}: #{status[2]}"
        end
      end
      module_function :load_events

      # Execute a chain of SQL commands, stopping as soon as
      # an error is encountered. At that point, it returns a
      # 'tuple' of the error class and message and the command
      # that caused the error
      #
      # Parameters:
      # +config+:: the hash of configuration options
      # +queries+:: the Redshift queries to execute sequentially
      #
      # Returns either an empty list on success, or on failure
      # a list of the form [query, err_class, err_message]
      def execute_queries(config, queries)

        conn = PG.connect({:host     => config[:storage][:host],
                           :dbname   => config[:storage][:database],
                           :port     => config[:storage][:port],
                           :user     => config[:storage][:username],
                           :password => config[:storage][:password]
                          })

        status = []
        queries.each do |q|
          begin
            conn.exec("#{q};")
          rescue PG::Error => err
            status = [q, err.class, err.message]
            break
          end
        end

        conn.finish
        return status
      end
      module_function :execute_queries

    end
  end
end
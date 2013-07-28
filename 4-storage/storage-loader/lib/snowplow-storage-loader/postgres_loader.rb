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
      NULL_STRING = ""

      # Loads the SnowPlow event files into Postgres.
      #
      # Parameters:
      # +config+:: the hash of configuration options 
      # +target+:: the configuration options for this target
      def load_events(config, target)
        puts "Loading Snowplow events into PostgreSQL..."

        queries = [
          "COPY #{target[:table]} FROM '#{config[:download][:folder]}' DELIMITER '#{EVENT_FIELD_SEPARATOR}' NULL '#{NULL_STRING}'",
          "VACUUM FULL ANALYZE #{target[:table]}"
        ]

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
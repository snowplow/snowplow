# Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2013 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

# Ruby module to support the load of SnowPlow events into Redshift
module SnowPlow
  module StorageLoader
    module RedshiftLoader

      # Constants for the load process
      EVENT_FIELD_SEPARATOR = "\\t"
      JISQL_PATH = File.join("jisql-2.0.11")

      # Loads the SnowPlow event files into Redshift.
      #
      # Parameters:
      # +config+:: the hash of configuration options 
      def load_events(config)
        puts "Loading SnowPlow events into Redshift..."

        # Assemble the relevant parameters for the bulk load query
        credentials = "aws_access_key_id=#{config[:aws][:access_key_id]};aws_secret_access_key=#{config[:aws][:secret_access_key]}"
        queries = ["copy #{config[:storage][:table]} from '#{config[:s3][:buckets][:in]}' credentials '#{credentials}' delimiter '#{EVENT_FIELD_SEPARATOR}'",
                   "analyze",
                   "vacuum"]

        status = execute_queries(config, queries)
        unless status == []
          raise DatabaseLoadError, "Error code #{status[1]} executing #{status[0]}: #{status[2]}"
        end 
      end
      module_function :load_events

      private

      # Execute a chain of SQL commands, stopping as soon as
      # an error is encountered. At that point, it returns a
      # 'tuple' of the error code, stdout/err and command
      # that caused the error
      #
      # Parameters:
      # +config+:: the hash of configuration options
      # +queries+:: the Redshift queries to execute sequentially
      #
      # Returns either an empty list on success, or on failure
      # a list of the form [query, ret_val, stdout_err]
      def execute_queries(config, queries)

        jdbc_url = "jdbc:postgresql://#{config[:storage][:endpoint]}:#{config[:storage][:port]}/#{config[:storage][:database]}"
        classpath = "%s/jisql-2.0.11.jar:%s/jopt-simple-3.2.jar:%s/postgresql-8.4-703.jdbc4.jar" % ([JISQL_PATH] * 3)
        username = config[:storage][:username]
        password = config[:storage][:password]

        status = []
        queries.each do |q|
          jisql_cmd = %Q!java -cp #{classpath} com.xigole.util.sql.Jisql -driver postgresql -cstring #{jdbc_url} -user #{username} -password #{password} -c \\; -query "#{q};"!
          stdout_err = `#{jisql_cmd} 2>&1` # Execute
          ret_val = $?.to_i

          unless ret_val == 0
            status = [q, ret_val, stdout_err]
            break
          end
        end

        return status
      end
      module_function

    end
  end
end
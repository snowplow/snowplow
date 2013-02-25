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
      JISQL_PATH = File.join("..", "jisql-2.0.11")

      def load_events(config)
        puts "Loading SnowPlow events into Redshift..."

        # Assemble the relevant parameters for the bulk load query
        jdbc_url = "jdbc:postgresql://#{config[:storage][:endpoint]}:#{config[:storage][:port]}/#{config[:storage][:database]}"
        credentials = "aws_access_key_id=#{config[:aws][:access_key_id]};aws_secret_access_key=#{config[:aws][:secret_access_key]}"
        query = "copy %s from '%s' credentials '%s' delimiter '%s';" % [config[:storage][:table], config[:s3][:buckets][:in], credentials, EVENT_FIELD_SEPARATOR]
        username = config[:storage][:username]
        password = config[:storage][:password]
           
        # Execute the following request at the command line:
        jisql_cmd = %Q!java -cp #{JISQL_PATH}/* com.xigole.util.sql.Jisql -driver postgresql -cstring #{jdbc_url} -user #{username} -password #{password} -c \\; -query "#{query}"!
        stdout_err = `#{jisql_cmd} 2>&1` # Execute
        ret_val = $?.to_i

        # Error handling
        unless ret_val == 0
          raise DatabaseLoadError, "Error code #{ret_val}: #{stdout_err}"
        end
      end
    end
  end
end
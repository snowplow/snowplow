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

      def load_events(config)
        puts "Loading SnowPlow events into Redshift..."

        # Assemble the relevant parameters for the bulk load query

        jdbc_url = "jdbc:postgresql://" + config[:targets][:Redshift][:endpoint] + ":" + config[:targets][:Redshift][:port].to_s + "/" + config[:targets][:Redshift][:database]

        source_in_s3 = config[:s3][:buckets][:in]

        output_table_in_redshift = config[:targets][:Redshift][:table]

        credentials = "'aws_access_key_id=" + config[:aws][:access_key_id] + ";aws_secret_access_key=" + config[:aws][:secret_access_key] + "'"

        delimiter = "'\\t'"

        query = "copy " + output_table_in_redshift + " from '" + source_in_s3 + "' credentials " + credentials + " delimiter " + delimiter + ";"

        username = config[:targets][:Redshift][:username]
        password = config[:targets][:Redshift][:password]

        # We need to execute the following request at the command line:

        cmd_line_request = %Q!java -cp 4-storage/storage-loader/jisql-2.0.11/lib/jisql-2.0.11.jar:4-storage/storage-loader/jisql-2.0.11/lib/jopt-simple-3.2.jar:4-storage/storage-loader/jisql-2.0.11/lib/postgresql-8.4-703.jdbc4.jar com.xigole.util.sql.Jisql -driver postgresql -cstring #{jdbc_url} -user #{username} -password #{password} -c \\; -query "#{query}"!

        stdout_err = `#{cmd_line_request} 2>&1` # Execute the cmd_line_request
        ret_val = $?.to_i
          unless ret_val == 0
            raise StorageLoader::Loader::DatabaseLoadError, "Error code #{ret_val}: #{stdout_err}"
          end
      end
    end
  end
end
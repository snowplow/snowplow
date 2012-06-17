# Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Alex Dean (mailto:alex@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

# Ruby module to support the two S3-related actions required by
# the daily ETL job:
# 1. Uploading the daily-etl.q HiveQL query to S3
# 2. Archiving the CloudFront log files by moving them into a separate bucket
module S3Utils

  def S3Utils.upload_query(config)
    puts "Uploading Hive query..."
  end

  def S3Utils.archive_logs(config)
    puts "Archiving CloudFront logs..."
  end
end

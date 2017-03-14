# Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2015 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

# Ruby 1.9.2 onwards doesn't add . into $LOAD_PATH by default - use require_relative instead
require_relative 'snowplow-storage-loader/snowplow'
require_relative 'snowplow-storage-loader/errors'
require_relative 'snowplow-storage-loader/contracts'
require_relative 'snowplow-storage-loader/config'
require_relative 'snowplow-storage-loader/file_tasks'
require_relative 'snowplow-storage-loader/s3_tasks'
require_relative 'snowplow-storage-loader/postgres_loader'
require_relative 'snowplow-storage-loader/shredded_type'
require_relative 'snowplow-storage-loader/redshift_loader'
require_relative 'snowplow-storage-loader/sanitization'

module Snowplow
  module StorageLoader
    NAME          = "snowplow-storage-loader"
    VERSION       = "0.10.0-rc4"
  end
end

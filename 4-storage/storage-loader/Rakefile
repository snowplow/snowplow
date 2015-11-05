#!/usr/bin/env rake

# Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Fred Blundun (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2015 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'rake/clean'

APP_NAME = 'snowplow-storage-loader'
JAR_PATH = "deploy/#{APP_NAME}.jar"
EXECUTABLE_PATH = "deploy/#{APP_NAME}"

task :build_executable do
	`rm -f #{EXECUTABLE_PATH}`
  `bundle exec warble`
  `cat bin/jarx-stub.sh #{JAR_PATH} > #{EXECUTABLE_PATH}`
  `chmod +x #{EXECUTABLE_PATH}`
  CLEAN.include(JAR_PATH)
end

task :default => [:build_executable, :clean]

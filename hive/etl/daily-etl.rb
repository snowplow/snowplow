#!/usr/bin/env ruby

# Copyright (c) 2012 Orderly Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

require 'optparse'
require 'date'

# Handle command-line arguments
OptionParser.new do |o|
  o.on('-j jarbucket') { |jar_bucket| $jar_bucket = jar_bucket }
  o.on('-i inbucket') { |in_bucket| $in_bucket = in_bucket }
  o.on('-o outbucket') { |out_bucket| $out_bucket = out_bucket }
  o.on('-a archivebucket') { |archive_bucket| $archive_bucket = archive_bucket }
  o.on('-h') { puts o; exit }
  o.parse!
end

# Determine yesterday's date
yesterday = (Date.today - 1).strftime('%Y-%m-%d')

# Now run the job via the Elastic MapReduce Command Line Tool
# TODO

#!/usr/bin/env ruby

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

# This script takes four command line arguments:
# 1. The EmrEtlRunner configuration file
# 2. The StorageLoader configuration file
# 3. The filepath where the combined configuration YAML should be saved
# 4. The filepath where the resolver JSON should be saved

require 'yaml'
require 'json'

# Deeply clone a hash
def clone(h)
  Marshal.load(Marshal.dump(h))
end

# Recursively change the keys of a YAML from symbols to strings
def recursive_stringify_keys(h)
  if h.class == [].class
    h.map {|key| recursive_stringify_keys(key)}
  elsif h.class == {}.class
    Hash[h.map {|k,v| [k.to_s, recursive_stringify_keys(v)]}]
  else
    h
  end
end

# Make macros safe
def escape_macros(f)
  File
    .read(f)
    .gsub(/{{(.*) "(.*)"}}/, "\"{{\\1 \\2}}\"")
end

# Restore macros
def unescape_macros(y)
  y.gsub(/'{{(.*) (.*)}}'/, "{{\\1 \"\\2\"}}")
end

eer_config = YAML.load(escape_macros(ARGV[0]))

sl_config = YAML.load(escape_macros(ARGV[1]))

yml_file = ARGV[2]

json_file = ARGV[3]

combined = clone(eer_config)

# Move the :s3 and :emr sections into the :aws section
combined[:aws][:s3] = combined[:s3]
combined.delete(:s3)
combined[:aws][:emr] = combined[:emr]
combined[:aws][:emr][:bootstrap_failure_tries] = 3
combined.delete(:emr)

# Multiple in buckets are now permitted
combined[:aws][:s3][:buckets][:raw][:in] = [* combined[:aws][:s3][:buckets][:raw][:in]]

combined[:aws][:s3][:buckets][:enriched][:archive] = sl_config[:s3][:buckets][:enriched][:archive]

combined[:aws][:s3][:buckets][:shredded][:archive] = sl_config[:s3][:buckets][:shredded][:archive]

combined[:collectors] = {format: eer_config[:etl][:collector_format]}

combined[:enrich] = combined[:etl]
combined[:enrich].delete(:collector_format)
# The original configuration has no output_compression argument
combined[:enrich][:output_compression] = 'NONE'
combined.delete(:etl)

combined[:aws][:s3][:buckets][:jsonpath_assets] = sl_config[:s3][:buckets][:jsonpath_assets]

combined[:storage] = clone(sl_config)
combined[:storage].delete(:aws)
combined[:storage].delete(:s3)

combined[:monitoring] = {
  tags: {},
  logging: combined[:logging],
  snowplow: nil
}
combined.delete(:logging)

# Separate the :resolver section into its own file
resolver = combined[:iglu]
combined.delete(:iglu)

combined = recursive_stringify_keys(combined)

resolver_json = JSON.pretty_generate(JSON.parse(resolver.to_json))

combined_configuration_yaml = unescape_macros(combined.to_yaml)

File.open(json_file, 'w') {|f| f.write resolver_json }
File.open(yml_file, 'w') {|f| f.write combined_configuration_yaml }

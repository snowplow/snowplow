# Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'plissken'

require 'contracts'
include Contracts

# Ruby module to support the load of Snowplow events into Redshift
module Snowplow
  module StorageLoader

    # Locates a shredded type in S3
    class ShreddedType

      @@snowplow_hosted_assets = "s3://snowplow-hosted-assets/4-storage/redshift-storage/jsonpath"

      # Searches S3 for all the files we can find
      # containing shredded types.
      #
      # Parameters:
      # +s3+:: the Fog object for accessing S3
      # +s3_path+:: the S3 path to the shredded type files
      # +schema+:: the schema that tables should live in
      Contract FogStorage, SluiceLocation, Maybe[String] => ArrayOf[ShreddedType]
      def self.discover_shredded_types(s3, s3_location, schema)

        Sluice::Storage::S3::list_files(s3, s3_location).map { |file|
          "s3://" + in_location.bucket + "/" + /^(?<s3_path>.*-)[^-]+-[^-]+\/[^\/]+$/.match(file.key)[:s3_path]
        }.uniq.map { |s3_objectpath|
          ShreddedType.new(s3_objectpath, schema)
        }
      end

      # Constructor
      #
      # Parameters:
      # +s3_path+:: the S3 path to the shredded type files
      # +schema+:: the schema that tables should live in
      Contract String, Maybe[String] => ShreddedType
      def initialize(s3_objectpath, schema)
        @s3_objectpath = s3_objectpath
        @schema = schema

        parts = /^.*\/(?<vendor>[^\/]+)\/(?<name>[^\/]+)\/(?<format>[^\/]+)\/(?<version_model>[^\/]+)-$/.match(s3_objectpath)
        @vendor = parts[:vendor]
        @name = parts[:name]
        @version_model = parts[:version_model]

        self
      end

      # Derives the table name in Redshift from the Iglu
      # schema key.
      #
      # Should convert:
      #   org.schema/WebPage/jsonschema/1
      # to:
      #   org_schema_web_page_1
      Contract None => String
      def table
        vendor = make_sql_safe(@vendor)
        name   = make_sql_safe(@name)
        schema = if @schema.nil? then "" else "#{@schema}." end
        "#{schema}#{vendor}_#{name}_#{@version_model}"
      end

      # Finds the JSON Paths file required to load
      # this shredded type into Redshift.
      #
      # Parameters:
      # +s3+:: the Fog object for accessing S3
      # +assets+:: path to custom assets (e.g. JSON Path files)
      Contract FogStorage, String => Maybe[String]
      def discover_jsonpaths_file(s3, assets)
        name = make_sql_safe(@name)
        file = "#{vendor}/#{name}_#{version}.json"

        # Look for it in the custom assets (if any)
        # TODO

        # Look for it in Snowplow's hosted assets
        # TODO

        nil # Not found
      end

    private

      # Replace any periods in vendor or name with underscore
      # Any camelCase or PascalCase to snake_case
      #
      # Parameters:
      # +value+:: the value to make SQL-safe
      Contract String => String
      def make_sql_safe(value)
        Hash.new.send(:underscore, value).tr('.', '_')
      end

    end

  end
end

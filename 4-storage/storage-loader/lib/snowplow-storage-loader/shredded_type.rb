# Copyright (c) 2012-2013 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2013 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'fog'
require 'plissken'

require 'contracts'
include Contracts

# Ruby module to support the load of Snowplow events into Redshift
module SnowPlow
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
      Contract FogStorage, String, Maybe[String] => ArrayOf[ShreddedType]
      def self.discover_shredded_types(s3, s3_path, schema)
        []
      end

      # Constructor
      #
      # Parameters:
      # +s3_path+:: the S3 path to the shredded type files
      # +schema+:: the schema that tables should live in
      # +run_id+:: the folder for this run, e.g.
      #            run=2014-06-24-08-19-52
      # +name+:: the shredded type's Iglu key's name
      # +vendor+:: the shredded type's Iglu key's vendor
      # +format+:: the shredded type's Iglu key's format
      # +version_model+:: the MODEL portion of the shredded type's Iglu key's SchemaVer version
      Contract String, Maybe[String], String, String, String, String => ShreddedType
      def initialize(s3_path, schema, run_id, name, vendor, format, version_model)
        @s3_path = s3_path
        @schema = schema
        @run_id = run_id
        @name = name
        @vendor = vendor
        @format = format
        @version_model = version_model

        self
      end

      # Creates an S3 objectpath for the COPY FROM JSON.
      # Note that Redshift COPY treats the S3 objectpath
      # as a prefixed path - i.e. you get "file globbing"
      # for free by specifying a partial folder path.
      Contract None => String
      def s3_objectpath
        # Trailing hyphen ensures we don't accidentally load
        # 11-x-x versions into a 1-x-x table
        "#{@s3_path}#{@run_id}/#{partial_key}-"
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
        schema = if @schema.nil? "" else "#{@schema}."
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

      # Returns the partial key
      Contract None => String
      def partial_key
        "#{name}/#{vendor}/#{format}/#{version_model}"
      end

    end

  end
end

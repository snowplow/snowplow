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

require 'sluice'
require 'plissken'
require 'contracts'

# Ruby module to support the load of Snowplow events into Redshift
module Snowplow
  module StorageLoader

    # Locates a shredded type in S3
    class ShreddedType

      include Contracts

      attr_reader :s3_objectpath, :table

      @@snowplow_hosted_assets_root = "s3://snowplow-hosted-assets"
      @@jsonpaths_path = "/4-storage/redshift-storage/jsonpaths/"

      @@jsonpaths_files = Hash.new # Our cache

      # Searches S3 for all the files we can find
      # containing shredded types.
      #
      # Parameters:
      # +s3+:: the Fog object for accessing S3
      # +s3_path+:: the S3 path to the shredded type files
      # +schema+:: the schema that tables should live in
      # +legacy+:: Whether we're using the old hadoop shred
      Contract FogStorage, Maybe[String], Maybe[String], Bool => ArrayOf[ShreddedType]
      def self.discover_shredded_types(s3, s3_path, schema, legacy)

        if s3_path.nil?
          []
        else
          loc = Sluice::Storage::S3::Location.new(s3_path)
          Sluice::Storage::S3::list_files(s3, loc).select { |file|
            # Ignore the altered enriched events in atomic-events - they are TSVs, not JSONs
            ! file.key.split('/').include? 'atomic-events'
          }.map { |file|
            # Strip off the final sub-folder's SchemaVer REVISION and ADDITION components
            "s3://" + loc.bucket + "/" + /^(?<s3_path>.*-)[^-]+-[^-]+\/[^\/]+$/.match(file.key)[:s3_path]
          }.uniq.map { |s3_objectpath|
            ShreddedType.new(s3_objectpath, schema, legacy)
          }
        end
      end

      # Constructor
      #
      # Parameters:
      # +s3_path+:: the S3 path to the shredded type files
      # +schema+:: the schema that tables should live in
      # +legacy+:: Whether we're using the old hadoop shred
      Contract String, Maybe[String], Bool => ShreddedType
      def initialize(s3_objectpath, schema, legacy)
        @s3_objectpath = s3_objectpath

        parts = if legacy
          /^.*\/(?<vendor>[^\/]+)\/(?<name>[^\/]+)\/(?<format>[^\/]+)\/(?<version_model>[^\/]+)-$/.match(s3_objectpath)
        else
          /^.*\/vendor=(?<vendor>[^\/]+)\/name=(?<name>[^\/]+)\/format=(?<format>[^\/]+)\/version=(?<version_model>[^\/]+)-$/.match(s3_objectpath)
        end
        @vendor = parts[:vendor]
        @name = parts[:name]
        @version_model = parts[:version_model]

        @table = get_table(schema, @vendor, @name, @version_model)
        self
      end

      # Finds the JSON Paths file required to load
      # this shredded type into Redshift.
      #
      # Parameters:
      # +s3+:: the Fog object for accessing S3
      # +assets+:: path to user's own JSON Path files, if provided
      Contract FogStorage, Maybe[String] => Maybe[String]
      def discover_jsonpaths_file(s3, assets)
        name = make_sql_safe(@name)
        file = "#{name}_#{@version_model}.json"

        # Check the cache first
        cache_key = "#{@vendor}/#{file}"
        unless @@jsonpaths_files[cache_key].nil?
          return @@jsonpaths_files[cache_key]
        end

        # Let's do the custom check first (allows a user to
        # override one of our JSON Path files with one of theirs)
        # Look for it in the custom assets (if any)
        unless assets.nil?
          custom_dir = "#{assets}#{@vendor}/"

          if file_exists?(s3, custom_dir, file)
            f = "#{custom_dir}#{file}"
            @@jsonpaths_files[cache_key] = f
            return f
          end
        end

        # Look for it in Snowplow's hosted assets, which
        # will definitely exist
        hosted_assets_bucket = get_hosted_assets_bucket(s3.region)
        snowplow_dir = "#{hosted_assets_bucket}#{@@jsonpaths_path}#{@vendor}/"
        if file_exists?(s3, snowplow_dir, file)
          f = "#{snowplow_dir}#{file}"
          @@jsonpaths_files[cache_key] = f
          return f
        end

        nil # Not found
      end

    private

      # Builds the region-appropriate bucket name for Snowplow's
      # hosted assets. Has to be region-specific as all aspects of
      # a Redshift COPY must be within the same region.
      #
      # Parameters:
      # +region+:: the AWS region to source hosted assets from
      Contract String => String
      def get_hosted_assets_bucket(region)
        suffix = if region.eql? "eu-west-1" then "" else "-#{region}" end
        "#{@@snowplow_hosted_assets_root}#{suffix}"
      end

      # Derives the table name in Redshift from the Iglu
      # schema key.
      #
      # Should convert:
      #   org.schema/WebPage/jsonschema/1
      # to:
      #   org_schema_web_page_1
      #
      # Parameters:
      # +schema+:: the schema that tables should live in
      # +vendor+:: the vendor of the schema
      # +name+:: the name of the schema
      # +version_model+:: the MODEL part of the schema's SchemaVer version
      Contract Maybe[String], String, String, String => String
      def get_table(schema, vendor, name, version_model)
        schema = if schema.nil? then "" else "#{schema}." end
        v = make_sql_safe(vendor)
        n = make_sql_safe(name)
        "#{schema}#{v}_#{n}_#{version_model}"
      end

      # Check if a file exists in a given directory
      Contract FogStorage, String, String => Bool
      def file_exists?(s3, directory, file)
        loc = Sluice::Storage::S3::Location.new(directory)
        dir = s3.directories.get(loc.bucket, prefix: loc.dir_as_path)
        (not dir.files.head(loc.dir_as_path + file).nil?)
      end

      # Replace any periods in vendor or name with underscore
      # Any camelCase or PascalCase to snake_case
      #
      # Parameters:
      # +value+:: the value to make SQL-safe
      Contract String => String
      def make_sql_safe(value)
        Hash.new.send(:underscore, value).tr('.', '_').tr('-', '_')
      end

    end

  end
end

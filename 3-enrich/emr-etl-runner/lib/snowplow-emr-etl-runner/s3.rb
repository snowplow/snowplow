# Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2018 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'aws-sdk-s3'
require 'contracts'
require 'pathname'
require 'uri'

module Snowplow
  module EmrEtlRunner
    module S3

      include Contracts

      # Check a location on S3 is empty.
      #
      # Parameters:
      # +client+:: S3 client
      # +location+:: S3 url of the folder to check for emptiness
      # +key_filter+:: filter to apply on the keys, filters folders and $folder$ files by default
      def empty?(client, location,
          key_filter = lambda { |k| !(k =~ /\/$/) and !(k =~ /\$folder\$$/) })
        bucket, prefix = parse_bucket_prefix(location)
        empty_impl(client, bucket, prefix, key_filter)
      end

      # Retrieve the alphabetically last object name satisfying a key filter.
      #
      # Parameters:
      # +client+:: S3 client
      # +location+:: S3 url of the folder to list the object names for
      # +key_filter+:: filter to apply on the keys
      def last_object_name(client, location, key_filter)
        bucket, prefix = parse_bucket_prefix(location)
        last_object_name_impl(client, bucket, prefix, key_filter)
      end

      # Extract the bucket and prefix from an S3 url.
      #
      # Parameters:
      # +location+:: the S3 url to parse
      Contract String => [String, String]
      def parse_bucket_prefix(location)
        u = URI.parse(location)
        return u.host, u.path[1..-1]
      end

    private

      def last_object_name_impl(client, bucket, prefix, key_filter, max_keys = 50)
        continuation_token = nil
        last = ""
        loop do
          response = list_objects(client, bucket, prefix, max_keys, continuation_token)
          new_last = response.contents
            .select { |c| key_filter[c.key] }
            .map { |c| c.key }
            .sort
            .last
          if not new_last.nil? and new_last > last
            last = new_last
          end
          continuation_token = response.next_continuation_token
          break unless response.is_truncated
        end
        last
      end

      def empty_impl(client, bucket, prefix, key_filter, max_keys = 50)
        continuation_token = nil
        filtered = []
        loop do
          response = list_objects(client, bucket, prefix, max_keys, continuation_token)
          filtered = response.contents.select { |c| key_filter[c.key] }
          continuation_token = response.next_continuation_token
          break unless filtered.empty? and response.is_truncated
        end
        if filtered.empty?
          true
        else
          false
        end
      end

      def list_objects(client, bucket, prefix, max_keys, token)
        options = {
          bucket: bucket,
          prefix: prefix,
          max_keys: max_keys,
        }
        options[:continuation_token] = token if !token.nil?
        begin
          retries ||= 0
          client.list_objects_v2(options)
        rescue AWS::S3::Errors::InternalError
          retries += 1
          sleep(2 ** retries * 0.1)
          retry if retries < 3
        end
      end

    end
  end
end

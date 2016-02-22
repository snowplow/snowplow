# Copyright (c) 2016 Ironside Group, Inc. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Steven Hu (mailto:shu@ironsidegroup.com)
# Copyright:: Copyright (c) 2016 Ironside Group, Inc.
# License::   Apache License Version 2.0

require 'sluice'
require 'contracts'
require 'uri'
require 'net/http'

# Ruby module to support the load of Snowplow events into DashDB
module Snowplow
  module StorageLoader
    module DashDBLoader

    include Contracts

    # Used to find the altered enriched events
    RUN_PATTERN = /(run=[0-9\-]+\/atomic-events)/

    # Loads the Snowplow atomic event files into DashDB table
    #
    # Parameters:
    # +config+:: the configuration options
    # +target+:: the configuration for this specific target
    # +snowplow_tracking_enabled+:: boolean indicating whether a monitor is present (from config[:monitoring][:snowplow])
    Contract Hash, Hash, Boolean => nil
    def load_atomic_events_into_dashdb(config, target, snowplow_tracking_enabled)
      puts "Loading Snowplow events #{target[:name]} (DashDB instance)..."

      run_folder = retrieve_run_folder(config, snowplow_tracking_enabled)

      response_value = create_run_dataworks_transfer(config, target, run_folder)

      if response_value != 200
        if snowplow_tracking_enabled
          Monitoring::Snowplow.instance.track_load_failed('Failure to transfer atomic events from S3 => DashDB, response #{response_value}.')
        end
      end

      if snowplow_tracking_enabled
        Monitoring::Snowplow.instance.track_load_succeeded()
      end

      nil
    end
    module_function :load_atomic_events_into_dashdb

    private

      # Call dataworks_proxy api endpoint with a request body that passes DashDB and S3 details.
      # The endpoint will create the S3 => DashDB activity and also kick it off.
      # ** Still waiting on DataWorks team to add new functionality where file pattern matching is allowed.
      #
      # Parameters:
      # +config+:: the configuration options
      # +target+:: the configuration for this specific target
      # +run_folder+:: the retrieved run folder from shredded/good
      Contract Hash, Hash, String => Num
      def create_run_dataworks_transfer(config, target, run_folder)
        url = URI.parse(target[dataworks_proxy])
        req = Net::HTTP::Post.new(url)
        req['Accept'] = 'application/json'
        req.body = '{
          "api_key":"#{target[:api_key]}",
          "host":"#{target[:host]}",
          "username":"#{target[:username]}",
          "password":"#{target[:password]}",
          "database":"#{target[:database]}",
          "schema":"#{target[:schema]}",
          "table":"#{target[:table]}",
          "s3_region":"#{config[:aws][:s3][:region]}",
          "s3_access_key":"#{config[:aws][:access_key_id]}",
          "s3_secret_key":"#{config[:aws][:secret_access_key]}",
          "s3_bucket":"#{config[:aws][:s3][:buckets][:shredded][:good]}/#{run_folder}"
        }'

        res = Net::HTTP.start(url.host, url.port, :use_ssl => url.scheme == 'https') { |http| http.request(req) }

        res.value
      end
      module_function :create_run_dataworks_transfer

      # Retrieve run folder from shredded/good. Max one folder in there at any given time.
      #
      # Parameters:
      # +config+:: the configuration options
      # +snowplow_tracking_enabled+:: boolean indicating whether a monitor is present (from config[:monitoring][:snowplow])
      Contract Hash, Boolean => String
      def retrieve_run_folder(config, snowplow_tracking_enabled)
        s3 = Sluice::Storage::S3::new_fog_s3_from(
          config[:aws][:s3][:region],
          config[:aws][:access_key_id],
          config[:aws][:secret_access_key])

        loc = Sluice::Storage::S3::Location.new(config[:aws][:s3][:buckets][:shredded][:good])

        shredded_good_run_filepath = Sluice::Storage::S3::list_files(s3, loc).find { |file| RUN_PATTERN.match(file.key) }

        if shredded_good_run_filepath.nil?
          if snowplow_tracking_enabled
            Monitoring::Snowplow.instance.track_load_failed('Cannot find atomic-events directory in shredded/good')
          end

          raise DatabaseLoadError, 'Cannot find atomic-events directory in shredded/good'
        end

        RUN_PATTERN.match(altered_enriched_filepath.key)[1]
      end
      module_function :retrieve_run_folder

    end
  end
end

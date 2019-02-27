# Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Joshua Beemster (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2019 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'contracts'
require 'pathname'
require 'uri'
require 'rest-client'

module Snowplow
  module EmrEtlRunner
    module EMR

      include Contracts

      # Attempts to find an active EMR JobFlow with a given name
      #
      # Parameters:
      # +client+:: EMR client
      # +name+:: EMR cluster name
      def get_emr_jobflow_id(client, name)
        # Marker is used for paginating through all results
        marker = nil
        emr_clusters = []

        loop do
          response = list_clusters(client, marker)
          emr_clusters = emr_clusters + response['Clusters'].select { |c| c['Name'] == name }
          marker = response['Marker'] if response.has_key?('Marker')
          break if marker.nil?
        end

        case emr_clusters.size
        when 0
          return nil
        when 1
          emr_cluster = emr_clusters.first
          if emr_cluster['Status']['State'] == "RUNNING"
            raise EmrClusterStateError, "EMR Cluster must be in WAITING state before new job steps can be submitted - found #{emr_cluster['Status']['State']}"
          end
          return emr_cluster['Id']
        else
          raise EmrDiscoveryError, "EMR Cluster name must be unique for safe discovery - found #{emr_clusters.size} with name #{name}"
        end
      end

      private

      def list_clusters(client, marker)
        options = {
          states: ["WAITING", "RUNNING"],
        }
        options[:marker] = marker unless marker.nil?
        begin
          retries ||= 0
          client.list_clusters(options)
        rescue Elasticity::ThrottlingException, RestClient::RequestTimeout, RestClient::InternalServerError, RestClient::ServiceUnavailable, RestClient::SSLCertificateNotVerified
          retries += 1
          sleep(2 ** retries * 1)
          retry if retries < 3
        end
      end

    end
  end
end

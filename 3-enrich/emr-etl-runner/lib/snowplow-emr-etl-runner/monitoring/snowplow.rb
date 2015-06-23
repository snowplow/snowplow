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

require 'singleton'
require 'snowplow-tracker'
require 'contracts'

module Snowplow
  module EmrEtlRunner
    module Monitoring
      class Snowplow

        include Singleton
        include Contracts

        # Constants
        PROTOCOL = "http"
        PORT = 80
        BUFFER_SIZE = 0
        APPLICATION_CONTEXT_SCHEMA = "iglu:com.snowplowanalytics.snowplow/application_context/jsonschema/1-0-0"
        JOB_STARTED_SCHEMA = "iglu:com.snowplowanalytics.snowplow/job_started/jsonschema/1-0-0"
        JOB_FAILED_SCHEMA = "iglu:com.snowplowanalytics.snowplow/job_failed/jsonschema/1-0-0"
        JOB_SUCCEEDED_SCHEMA = "iglu:com.snowplowanalytics.snowplow/job_succeeded/jsonschema/1-0-0"

        # Parameters
        @@method = "get"
        @@collector_uri = nil
        @@app_id = nil

        # Parameterize a new Snowplow singleton
        # Approach taken from http://stackoverflow.com/a/6894791/255627
        Contract ConfigHash => nil
        def self.parameterize(config)

          cm = config[:monitoring]
          cms = cm[:snowplow]
          @@method = cms[:method].downcase || @@method
          @@collector_uri = cms[:collector] # Could be nil
          @@app_id = cms[:app_id] # Could be nil
          
          @@app_context = Snowplow.as_self_desc_hash(APPLICATION_CONTEXT_SCHEMA, {
            :name => NAME,
            :version => VERSION,
            :tags => cm[:tags],
            :logLevel => config[:monitoring][:logging][:level]
            })

          nil
        end

        # Create our Snowplow singleton
        Contract None => nil
        def initialize
          @tracker =
            if @@collector_uri
              emitter = SnowplowTracker::Emitter.new(@@collector_uri, {
                :protocol => PROTOCOL,
                :method => @@method,
                :port => PORT,
                :buffer_size => BUFFER_SIZE
              })

              SnowplowTracker::Tracker.new(emitter, nil, nil, @@app_id)
            else
              nil
            end

          nil
        end

        # Helper to make a self-describing hash
        Contract String, {} => { :schema => String, :data => {} }
        def self.as_self_desc_hash(schema, data)
          { :schema => schema,
            :data => data
          }
        end

        # Track a job started event
        # TODO: decide on data field
        Contract None => SnowplowTracker::Tracker
        def track_job_started
          @tracker.track_unstruct_event(
            Snowplow.as_self_desc_hash(
              JOB_STARTED_SCHEMA,
              {}
            ),
            [@@app_context]
          )
        end

        # Track a job succeeded event
        # TODO: decide on data field
        Contract None => SnowplowTracker::Tracker
        def track_job_succeeded
          @tracker.track_unstruct_event(
            Snowplow.as_self_desc_hash(
              JOB_SUCCEEDED_SCHEMA,
              {}
            ),
            [@@app_context]
          )
        end

        # Track a job failed event
        # TODO: decide on data field
        Contract None => SnowplowTracker::Tracker
        def track_job_failed
          @tracker.track_unstruct_event(
            Snowplow.as_self_desc_hash(
              JOB_FAILED_SCHEMA,
              {}
            ),
            [@@app_context]
          )
        end

      end
    end
  end
end

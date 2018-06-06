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
require 'elasticity'
require 'snowplow-tracker'
require 'contracts'
require 'date'

module Snowplow
  module EmrEtlRunner
    module Monitoring
      class Snowplow

        include Singleton
        include Contracts

        # Constants
        PORT = 80
        BUFFER_SIZE = 0
        APPLICATION_CONTEXT_SCHEMA = "iglu:com.snowplowanalytics.monitoring.batch/application_context/jsonschema/1-0-0"
        JOB_STATUS_SCHEMA = "iglu:com.snowplowanalytics.monitoring.batch/emr_job_status/jsonschema/1-0-0"
        STEP_STATUS_SCHEMA = "iglu:com.snowplowanalytics.monitoring.batch/jobflow_step_status/jsonschema/1-0-0"
        JOB_STARTED_SCHEMA = "iglu:com.snowplowanalytics.monitoring.batch/emr_job_started/jsonschema/1-0-0"
        JOB_FAILED_SCHEMA = "iglu:com.snowplowanalytics.monitoring.batch/emr_job_failed/jsonschema/1-0-0"
        JOB_SUCCEEDED_SCHEMA = "iglu:com.snowplowanalytics.monitoring.batch/emr_job_succeeded/jsonschema/1-0-0"

        # Parameters
        @@method = "get"
        @@protocol = "http"
        @@collector_uri = nil
        @@app_id = nil

        # Parameterize a new Snowplow singleton
        # Approach taken from http://stackoverflow.com/a/6894791/255627
        Contract ConfigHash => nil
        def self.parameterize(config)

          cm = config[:monitoring]
          cms = cm[:snowplow]
          @@method = cms[:method].downcase || @@method
          @@protocol = if not cms[:protocol].nil?
            cms[:protocol].downcase
          else
            @@protocol
          end
          @@collector_uri = cms[:collector] # Could be nil
          @@app_id = cms[:app_id] # Could be nil

          @@app_context = SnowplowTracker::SelfDescribingJson.new(APPLICATION_CONTEXT_SCHEMA, {
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
                :protocol => @@protocol,
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

        # Make a timestamp returned by a job status query compatible with JSON schema
        Contract Maybe[Time] => Maybe[String]
        def to_jsonschema_compatible_timestamp(time)
          if time.nil?
            nil
          else
            time.strftime('%Y-%m-%dT%H:%M:%SZ')
          end
        end

        # Context for the entire job
        Contract Elasticity::JobFlow => SnowplowTracker::SelfDescribingJson
        def get_job_context(jobflow)
          status = jobflow.cluster_status
          SnowplowTracker::SelfDescribingJson.new(
            JOB_STATUS_SCHEMA,
            {
              :name => status.name,
              :jobflow_id => jobflow.jobflow_id,
              :state => status.state,
              :created_at => to_jsonschema_compatible_timestamp(status.created_at),
              :ended_at => to_jsonschema_compatible_timestamp(status.ended_at),
              :last_state_change_reason => status.last_state_change_reason
            }
          )
        end

        # One context per job step
        Contract Elasticity::JobFlow => ArrayOf[SnowplowTracker::SelfDescribingJson]
        def get_job_step_contexts(jobflow)
          jobflow.cluster_step_status.map { |step|
            SnowplowTracker::SelfDescribingJson.new(
              STEP_STATUS_SCHEMA,
              {
                :name => step.name,
                :state => step.state,
                :created_at => to_jsonschema_compatible_timestamp(step.created_at),
                :started_at => to_jsonschema_compatible_timestamp(step.started_at),
                :ended_at => to_jsonschema_compatible_timestamp(step.ended_at)
              }
            )
          }
        end

        # Track a job started event
        Contract Elasticity::JobFlow => SnowplowTracker::Tracker
        def track_job_started(jobflow)
          @tracker.track_unstruct_event(
            SnowplowTracker::SelfDescribingJson.new(
              JOB_STARTED_SCHEMA,
              {}
            ),
            [@@app_context, get_job_context(jobflow)] + get_job_step_contexts(jobflow)
          )
        end

        # Track a job succeeded event
        Contract Elasticity::JobFlow => SnowplowTracker::Tracker
        def track_job_succeeded(jobflow)
          @tracker.track_unstruct_event(
            SnowplowTracker::SelfDescribingJson.new(
              JOB_SUCCEEDED_SCHEMA,
              {}
            ),
            [@@app_context, get_job_context(jobflow)] + get_job_step_contexts(jobflow)
          )
        end

        # Track a job failed event
        Contract Elasticity::JobFlow => SnowplowTracker::Tracker
        def track_job_failed(jobflow)
          @tracker.track_unstruct_event(
            SnowplowTracker::SelfDescribingJson.new(
              JOB_FAILED_SCHEMA,
              {}
            ),
            [@@app_context, get_job_context(jobflow)] + get_job_step_contexts(jobflow)
          )
        end

      end
    end
  end
end

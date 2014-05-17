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

require 'contracts'
include Contracts

module Snowplow
  module EmrEtlRunner
    class Runner

      include Logging

      # Initialize the class.
      Contract ArgsConfigTuple => Runner
      def initialize(args_config)

        @args, @config = args_config

        # Let's set our logging level immediately
        Logging::set_level config[:logging][:level]
      end

      # Our core flow
      Contract None => nil
      def run

        # Now our core flow
        unless @args[:skip].include?('staging')
          unless S3Tasks.stage_logs_for_emr(@args, @config)
            logger.info "No Snowplow logs to process since last run, exiting"
            exit 0
          end
        end

        unless @args[:skip].include?('emr')
          job = EmrJob.new(@args[:debug], @config)
          job.run()
        end

        unless @config[:skip].include?('archive')
          S3Tasks.archive_logs(@config)
        end

        logger.info "Completed successfully"
      end

    end
  end
end

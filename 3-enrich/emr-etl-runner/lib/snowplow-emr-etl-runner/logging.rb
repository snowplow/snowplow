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

require 'logger'

require 'contracts'
include Contracts

module Snowplow
  module EmrEtlRunner
    module Logging

      $stdout.sync = true

      # Get the Logger
      Contract None => Logger
      def logger
        Logging.logger
      end

      # Global, memoized, lazy initialized instance of a logger
      Contract None => Logger 
      def self.logger
        @logger ||= Logger.new($stdout)
      end

      # Log a fatal Exception
      Contract Exception => nil
      def self.fatal_with(exception)
        logger.fatal("\n\n#{exception.class} (#{exception.message}):\n    " +
                     exception.backtrace.join("\n    ") +
                     "\n\n")
        nil
      end

      # Set the logging level
      Contract String => nil
      def self.set_level(level)
        logger.level = Logger.const_get level.upcase
        nil
      end
    end
  end
end

# Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'elasticity'

module Snowplow
  module EmrEtlRunner

    # TODO: move this into Elasticity at some point
    class ScaldingStep < Elasticity::CustomJarStep

      def initialize(jar, main_class, options)
        @name = 'Elasticity Scalding Step'
        @jar = jar
        @action_on_failure = 'TERMINATE_JOB_FLOW'

        @arguments = [ main_class, '--hdfs' ]        
        options.each do |argument, value|
          @arguments << "--#{argument}" << value
        end
      end
    end

  end
end

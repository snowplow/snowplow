# Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2017 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

module Snowplow
  module EmrEtlRunner
    module Lock
      module Lock

        def try_lock
          raise RuntimeError, '#try_lock has to be defined in all locks'
        end

        def unlock
          raise RuntimeError, '#try_lock has to be defined in all locks'
        end

      end
    end
  end
end
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

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2019 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'diplomat'

module Snowplow
  module EmrEtlRunner
    module Lock
      class ConsulLock

        include Snowplow::EmrEtlRunner::Lock::Lock

        attr_accessor :path

        def initialize(consul_address, path)
          Diplomat.configure do |config|
            config.url = consul_address
          end
          @path = path
        end

        def try_lock
          k = Diplomat::Kv.get(@path, nil, :return, :return)
          if k != ''
            raise LockHeldError, "Lock already held at #{@path}"
          end
          pid = Process.pid
          success = Diplomat::Kv.put(@path, pid.to_s, nil)
          if not success
            raise Error, "Could not store key at #{@path}"
          end
        end

        def unlock
          Diplomat::Kv.delete(@path)
        end

      end
    end
  end
end
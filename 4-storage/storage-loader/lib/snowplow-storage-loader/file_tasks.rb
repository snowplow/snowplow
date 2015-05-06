# Copyright (c) 2013 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2013 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

# Ruby module to support the load of Snowplow events into local databases.
module Snowplow
  module StorageLoader
    module FileTasks

      # Empties the download folder now that the events
      # have been loaded
      #
      # Parameters:
      # +folder+:: the folder containing the files to delete 
      def delete_events(folder)
        FileUtils.rm_rf("#{folder}/.", :secure => true)
      end
      module_function :delete_events

    end
  end
end
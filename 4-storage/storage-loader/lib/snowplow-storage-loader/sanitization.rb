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

module Snowplow
  module StorageLoader
    module Sanitization

      # Replaces any substring of length at least min_replaced_substring_length of an error message
      # if it is also a substring of any credential string
      def sanitize_message(message, credentials, min_replaced_substring_length=7)
        credentials.each do |cred|
          (0..(cred.length - min_replaced_substring_length)).each do |start|
            (cred.length.downto(start + min_replaced_substring_length)).each do |finish|
              message = message.gsub(cred[start...finish], 'x' * (finish - start))
            end
          end
        end
        message
      end
      module_function :sanitize_message

    end
  end
end

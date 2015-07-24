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

# Author::    Fred Blundun (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'spec_helper'

Runner = Snowplow::EmrEtlRunner::Runner

describe Runner do

  it 'adds trailing slashes to all buckets' do
    Runner.add_trailing_slashes({
      '1' => 'a',
      '2' => {
        '3' => 'b',
        '4' => 'c'
      },
      '5' => ['d', [{
        '6' => 'e'
      }]]
    }).should == {
      '1' => 'a/',
      '2' => {
        '3' => 'b/',
        '4' => 'c/'
      },
      '5' => ['d/', [{
        '6' => 'e/'
      }]]
    }
  end

end

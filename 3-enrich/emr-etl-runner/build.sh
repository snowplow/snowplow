#!/bin/bash

# Copyright (c) 2015-2017 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2015 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

source ~/.rvm/scripts/rvm

rvm install jruby-9.1.6.0
rvm use --default jruby-9.1.6.0
gem install bundler
bundle install
echo 'Running RSpec'
rspec
rake

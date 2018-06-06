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

source "https://rubygems.org"
ruby "2.3.1"

# ErmEtlRunner is a Ruby app (not a RubyGem)
# built with Bundler, so we add in the
# RubyGems it requires here.
gem "contracts", "~> 0.9", "<= 0.11"
gem "elasticity", "~> 6.0.12"
gem "avro", "~> 1.8.1"
gem "awrence", "~> 0.1.0"
gem "snowplow-tracker", "~> 0.5.2"
gem "iglu-ruby-client", ">= 0.1.0"
gem "aws-sdk-s3", "~> 1"
gem "diplomat", "~> 2.0.1"

group :development do
  gem "rspec", "~> 3.5.0"
  gem "coveralls"

  gem "warbler" if RUBY_PLATFORM == 'java'
end

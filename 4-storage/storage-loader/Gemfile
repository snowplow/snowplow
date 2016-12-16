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

# StorageLoader is a Ruby app (not a RubyGem)
# built with Bundler, so we add in the
# RubyGems it requires here.
gem "sluice", "~> 0.4.0"
gem 'mime-types' # Really this should be in Sluice. Somehow doesn't break EmrEtlRunner
gem 'jdbc-postgres'
gem "plissken", "~> 0.1.0"
gem "contracts", "~> 0.9", "<= 0.11"
gem "snowplow-tracker", "~> 0.5.2"
gem "aws-sdk", "~> 2"
gem "iglu-ruby-client", ">= 0.1.0"
gem "warbler"

group :development do
  gem "rspec", "~> 2.14", ">= 2.14.1"
  gem "coveralls"

  gem "warbler" if RUBY_PLATFORM == 'java'
end

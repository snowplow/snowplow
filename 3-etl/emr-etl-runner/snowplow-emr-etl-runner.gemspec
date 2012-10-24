# Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

# -*- encoding: utf-8 -*-
require File.expand_path('../lib/snowplow-emr-etl-runner/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Alex Dean <support@snowplowanalytics.com>"]
  gem.email         = ["support@snowplowanalytics.com"]
  gem.summary       = %q{Runs the SnowPlow Hive ETL on EMR}
  gem.description   = %q{A Ruby gem to run SnowPlow's Hive ETL job on Amazon Elastic MapReduce. Uses Elasticity and Fog}
  gem.homepage      = "http://snowplowanalytics.com"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = SnowPlow::EmrEtlRunner::SCRIPT_NAME
  gem.version       = SnowPlow::EmrEtlRunner::VERSION
  gem.platform      = Gem::Platform::RUBY
  gem.require_paths = ["lib"]

  # Only dependencies
  gem.add_dependency 'elasticity', '~> 2.5'
  gem.add_dependency 'fog', '~> 1.6.0'
end
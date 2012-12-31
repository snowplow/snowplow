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
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'snowplow-storage-loader'

Gem::Specification.new do |gem|
  gem.authors       = ["Alex Dean <support@snowplowanalytics.com>"]
  gem.email         = ["support@snowplowanalytics.com"]
  gem.summary       = %q{Runs the SnowPlow Hive ETL on EMR}
  gem.description   = %q{A Ruby application to load SnowPlow events into various databases and big data stores}
  gem.homepage      = "http://snowplowanalytics.com"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = SnowPlow::StorageLoader::NAME
  gem.version       = SnowPlow::StorageLoader::VERSION
  gem.platform      = Gem::Platform::RUBY
  gem.require_paths = ["lib"]

  # Dependencies
  gem.add_dependency 'infobright-loader', '~> 0.0.1'
  gem.add_dependency 'sluice', '~> 0.0.6'
end

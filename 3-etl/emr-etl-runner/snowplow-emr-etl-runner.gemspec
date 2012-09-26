# -*- encoding: utf-8 -*-
require File.expand_path('../lib/snowplow-emr-etl-runner/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Alex Dean <support@snowplowanalytics.com>"]
  gem.email         = ["support@snowplowanalytics.com"]
  gem.summary       = %q{Runs the SnowPlow Hive ETL on EMR}
  gem.description   = %q{A Bundler RubyGem to run SnowPlow's Hive ETL job on Amazon Elastic MapReduce. Uses Elasticity and Fog}
  gem.homepage      = "http://snowplowanalytics.com"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = SnowPlow::EmrEtlRunner::SCRIPT_NAME
  gem.version       = SnowPlow::EmrEtlRunner::VERSION
  gem.platform      = Gem::Platform::RUBY
  gem.require_paths = ["lib"]

  gem.add_dependency 'elasticity', '~> 2.4'
  gem.add_dependency 'fog', '~> 1.6.0'
  gem.add_dependency 's3', '0.3.11'
  gem.add_dependency 'aws-s3', '0.6.3'
  # TODO: remove s3 when no longer used
end

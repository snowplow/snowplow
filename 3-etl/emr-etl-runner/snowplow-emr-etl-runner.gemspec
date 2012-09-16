# -*- encoding: utf-8 -*-
require File.expand_path('../lib/snowplow-emr-etl-runner/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Alex Dean"]
  gem.email         = ["support@snowplowanalytics.com"]
  gem.description   = %q{A Bundler RubyGem to run SnowPlow's Hive ETL job on Amazon Elastic MapReduce. Uses Elasticity and qoobaa/s3}
  gem.summary       = %q{Runs the SnowPlow Hive ETL on EMR}
  gem.homepage      = "http://snowplowanalytics.com"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = SnowPlow::EmrEtlRunner::SCRIPT_NAME
  gem.require_paths = ["lib"]
  gem.version       = SnowPlow::EmrEtlRunner::VERSION

  gem.add_dependency 'elasticity', '~> 2.4'
  gem.add_dependency 's3', '0.3.11'
end

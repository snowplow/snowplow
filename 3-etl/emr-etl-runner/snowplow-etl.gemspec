# -*- encoding: utf-8 -*-
require File.expand_path('../lib/snowplow-etl/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Alex Dean"]
  gem.email         = ["alex@snowplowanalytics.com"]
  gem.description   = %q{A Ruby gem (built using Bundler) to run the daily ETL job for SnowPlow. Uses the Ruby elastic-mapreduce client to run the HiveQL ETL script, and uses AWS:S3 to archive the processed CloudFront log files}
  gem.summary       = %q{Runs the daily ETL job for SnowPlow}
  gem.homepage      = "http://snowplowanalytics.com"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "snowplow-etl"
  gem.require_paths = ["lib"]
  gem.version       = SnowPlow::Etl::VERSION

  gem.add_dependency 'elasticity', '~> 2.2'
  gem.add_dependency 'aws-s3', '~> 0.6.3'
end

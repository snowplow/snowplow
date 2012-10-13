# SnowPlow::EmrEtlRunner

## Introduction

SnowPlow::EmrEtlRunner is a Ruby gem (built with [Bundler] [bundler]) to run SnowPlow's Hive-based ETL (extract, transform, load) process on [Amazon Elastic MapReduce] [amazon-emr] with minimum fuss.

## Deployment and configuration

For detailed instructions on setting up EmrEtlRunner on your server, please see the [Deploying the EMR-ETL-Runner] [deploying-emr-etl-runner] guide on the SnowPlow Analytics wiki.

## Contributing

If you want to hack on EmrEtlRunner locally, first make sure that you have the following dependencies installed:

1. Git installed. Please see the [Git Installation Guide] [git-install] as needed  
2 **Ruby**. Please see the [Ruby Download Instructions] [ruby-install] as needed
3. **RubyGems**. Please see the [RubyGems Installation Instructions] [rubygems-install]
   as needed
4. **Nokogiri**. Please see the [Installing Nokogiri Guide] [nokogiri-install] as needed
5. **Bundler**. This is a one-liner: `gem install bundler`

Now checkout the repository:

    $ git clone git://github.com/snowplow/snowplow.git
    
Next install the SnowPlow::ETL gem on your system:

    $ cd snowplow/3-etl/emr-etl-runner
    $ gem build snowplow-etl.gemspec 
    $ sudo gem install snowplow-etl-0.0.1.gem
    $ bundle install

Finally, test that the gem was installed successfully:

    $ bundle exec snowplow-etl --version
    snowplow-etl 0.0.1

Note that the `bundle exec` command will only work when you are inside the 
`emr-etl-runner` folder.

That's it! Next, head over to the [Deploying the EMR-ETL-Runner] [deploying-emr-etl-runner] guide for help on configuring and running your local copy of EmrEtlRunner.

Happy hacking!

## Credits and thanks

SnowPlow::EmrEtlRunner was primarily developed by [Alex Dean] [alexanderdean] ([SnowPlow Analytics] [snowplow-analytics]), with substantial contributions from [Michael Tibben] [mtibben] ([99designs] [99designs]). Thank you Michael!

EmrEtlRunner in turn depends heavily on [Rob Slifka] [rslifka]'s excellent [Elasticity] [elasticity] Ruby gem, which provides programmatic access to Amazon's Elastic Map Reduce service. Huge thanks to Rob for writing Elasticity!

## Copyright and license

SnowPlow is copyright 2012 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[bundler]: http://gembundler.com/
[amazon-emr]: http://aws.amazon.com/elasticmapreduce/
[deploying-emr-etl-runner]: https://github.com/snowplow/snowplow/wiki/Deploying-the-EMR-ETL-Runner

[git-install]: http://git-scm.com/book/en/Getting-Started-Installing-Git
[ruby-install]: http://www.ruby-lang.org/en/downloads/
[nokogiri-install]: http://nokogiri.org/tutorials/installing_nokogiri.html
[rubygems-install]: http://docs.rubygems.org/read/chapter/3

[alexanderdean]: https://github.com/alexanderdean
[snowplow-analytics]: http://snowplowanalytics.com
[mtibben]: https://github.com/mtibben
[99designs]: http://99designs.com
[rslifka]: https://github.com/rslifka
[elasticity]: https://github.com/rslifka/elasticity

[license]: http://www.apache.org/licenses/LICENSE-2.0
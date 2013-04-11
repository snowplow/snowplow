# SnowPlow::EmrEtlRunner

## Introduction

SnowPlow::EmrEtlRunner is a Ruby application (built with [Bundler] [bundler]) to run SnowPlow's Hive-based ETL (extract, transform, load) process on [Amazon Elastic MapReduce] [amazon-emr] with minimum fuss.

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [techdocs] | [Setup Guide] [setup] | _coming soon_                        |

## Credits and thanks

SnowPlow::EmrEtlRunner was primarily developed by [Alex Dean] [alexanderdean] ([SnowPlow Analytics] [snowplow-analytics]), with very substantial contributions from [Michael Tibben] [mtibben] ([99designs] [99designs]). Huge thanks Michael!

EmrEtlRunner in turn depends heavily on [Rob Slifka] [rslifka]'s excellent [Elasticity] [elasticity] Ruby gem, which provides programmatic access to Amazon EMR. Big thanks to Rob for writing Elasticity!

## Copyright and license

SnowPlow::EmrEtlRunner is copyright 2012-2013 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[bundler]: http://gembundler.com/
[amazon-emr]: http://aws.amazon.com/elasticmapreduce/
[deploying-emr-etl-runner]: https://github.com/snowplow/snowplow/wiki/Deploying-EmrEtlRunner

[alexanderdean]: https://github.com/alexanderdean
[snowplow-analytics]: http://snowplowanalytics.com
[mtibben]: https://github.com/mtibben
[99designs]: http://99designs.com
[rslifka]: https://github.com/rslifka
[elasticity]: https://github.com/rslifka/elasticity

[license]: http://www.apache.org/licenses/LICENSE-2.0
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[techdocs]: https://github.com/snowplow/snowplow/wiki/EmrEtlRunner
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-EmrEtlRunner
# SnowPlow::StorageLoader

## Introduction

SnowPlow::StorageLoader is a Ruby application (built with [Bundler] [bundler]) to load SnowPlow event data into various databases and "big data" platforms. Initially StorageLoader supports one storage 'target': 



 (extract, transform, load) process on [Amazon Elastic MapReduce] [amazon-emr] with minimum fuss.

## Deployment and configuration

For detailed instructions on installing, running and scheduling EmrEtlRunner on your server, please see the [Deploying EmrEtlRunner] [deploying-emr-etl-runner] guide on the SnowPlow Analytics wiki.

## Contributing

We will be adding a guide to contributing soon.

## Credits and thanks

SnowPlow::StorageLoader was developed by [Alex Dean] [alexanderdean] ([SnowPlow Analytics] [snowplow-analytics])

StorageLoader in turn depends heavily on [Sluice] [sluice], a Ruby toolkit for cloud-friendly ETL, written by [Alex Dean] [alexanderdean] ([SnowPlow Analytics] [snowplow-analytics]) and [Michael Tibben] [mtibben] ([99designs] [99designs]).

## Copyright and license

SnowPlow::StorageLoader is copyright 2012 SnowPlow Analytics Ltd.

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

[infobright]: http://www.infobright.org/

[sluice]: https://github.com/snowplow/sluice

[mtibben]: https://github.com/mtibben
[99designs]: http://99designs.com

[license]: http://www.apache.org/licenses/LICENSE-2.0
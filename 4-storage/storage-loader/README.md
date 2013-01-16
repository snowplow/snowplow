# SnowPlow::StorageLoader

## Introduction

SnowPlow::StorageLoader is a Ruby application (built with [Bundler] [bundler]) to load SnowPlow event data into various databases and "big data" platforms. Initially StorageLoader supports one storage target: [Infobright Community Edition] [infobright] (ICE).

In the future we plan on supporting other storage targets, including:

* [Postgres] [postgres]
* [MySQL] [mysql]
* [Google BigQuery] [bigquery]
* [SkyDB] [skydb]

# Technical documentation

We will be adding the technical documentation for StorageLoader on the Wiki soon.

## Deployment and configuration

For detailed instructions on installing, running and scheduling StorageLoader on your server, please see the [StorageLoader Setup Guide] [storage-loader-setup-guide] on the Wiki.

## Contributing

We will be adding a guide to contributing to StorageLoader on the Wiki soon.

## Credits and thanks

StorageLoader was developed by [Alex Dean] [alexanderdean] ([SnowPlow Analytics] [snowplow-analytics]).

StorageLoader in turn depends heavily on [Sluice] [sluice], a Ruby toolkit for cloud-friendly ETL, written by [Alex Dean] [alexanderdean] ([SnowPlow Analytics] [snowplow-analytics]) and [Michael Tibben] [mtibben] ([99designs] [99designs]).

## Copyright and license

SnowPlow::StorageLoader is copyright 2012-2013 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[bundler]: http://gembundler.com/

[infobright]: http://www.infobright.org/
[postgres]: http://www.postgresql.org/
[mysql]: http://www.mysql.com/
[bigquery]: https://developers.google.com/bigquery/
[skydb]: https://github.com/skydb/sky

[sluice]: https://github.com/snowplow/sluice

[alexanderdean]: https://github.com/alexanderdean
[snowplow-analytics]: http://snowplowanalytics.com
[mtibben]: https://github.com/mtibben
[99designs]: http://99designs.com

[license]: http://www.apache.org/licenses/LICENSE-2.0
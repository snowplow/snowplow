# SnowPlow

## Introduction

SnowPlow is the world's most powerful web analytics platform. It does three things:

* Identifies users, and tracks the way they engage with one or more websites
* Stores the associated data in a scalable “clickstream” data warehouse
* Makes it possible to leverage a big data toolset (e.g. Hadoop, Pig, Hive) to analyse that data

To find out more, read Keplar's blog post [introducing SnowPlow] [blogpost]. The rest of the
documentation in this repository focuses on the technical aspects of SnowPlow.

## Contents

Contents of this repository are as follows:

* The root folder contains this README and the [Apache License, Version 2.0] [license]
* `docs` contains the technical documentation for this project. See the next section for more details
* `tracker` contains everything related to SnowPlow's JavaScript tracker, `snowplow.js`
* `hive` contains everything related to running Hive analytics on the SnowPlow data

## Documentation

There is a growing set of documentation for SnowPlow.

The most comprehensive set of documenetation can be found on the [SnowPlow wiki][snowplow-wiki]. This includes:

* [Technical documentation including setup instructions][setup-snowplow-instructions]

* [Introduction] [intro]
* [Technical FAQ] [techfaq]
* [Integrating snowplow.js into your site] [integrating]
* [SnowPlow for ad tracking] [adtracking]
* [Self-hosting SnowPlow] [selfhosting]
* [Setting up Amazon MapReduce] [mapreduce]
* [Running a Hive interactive session] [hivesession]
* [Introduction to the SnowPlow Hive tables] [hivetables]

Additionally it is worth reading the technical READMEs for the sub-projects:

* The [README] [jsreadme] for the `snowplow.js` JavaScript tracker
* The [README] [serdereadme] for the `snowplow-log-deserializers` project
* The [README] [recipesreadme] for the SnowPlow Hive recipes

## Roadmap

Planned items on the roadmap are as follows:

* Making `snowplow.js` available over SSL (not currently working)
* Writing and opensourcing some standard Hive 'recipes'
* Releasing schedulable Hive scripts for aggregating daily data

## Contributors

* [Alex Dean](https://github.com/alexanderdean)
* [Yali Sassoon](https://github.com/yalisassoon)
* [Anthon Pang](https://github.com/robocoder)

Original concept for SnowPlow inspired by [Radek Maciaszek](https://github.com/rathko).

## Copyright and license

SnowPlow is copyright 2012 Orderly Ltd. Significant portions of `snowplow.js`
are copyright 2010 Anthon Pang.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[blogpost]: http://www.keplarllp.com/blog/2012/02/introducing-snowplow-the-worlds-most-powerful-web-analytics-platform

[snowplow-wiki]: //snowplow/snowplow/wiki
[setup-snowplow-instructions]: //snowplow/snowplow/wiki

[intro]: /snowplow/snowplow/blob/master/docs/01_introduction.md
[techfaq]: /snowplow/snowplow/blob/master/docs/02_technical_faq.md
[integrating]: /snowplow/snowplow/blob/master/docs/03_integrating_snowplowjs.md
[adtracking]: /snowplow/snowplow/blob/master/docs/03a_snowplowjs_for_ads.md
[selfhosting]: /snowplow/snowplow/blob/master/docs/04_selfhosting_snowplow.md
[mapreduce]: /snowplow/snowplow/blob/master/docs/05_setting_up_amazon_elastic_mapreduce.md
[hivesession]: /snowplow/snowplow/blob/master/docs/06_running_hive_interactive_session.md
[hivetables]: /snowplow/snowplow/blob/master/docs/07_snowplow_hive_tables_introduction.md

[jsreadme]: /snowplow/snowplow/blob/master/tracker/README.md
[serdereadme]: https://github.com/snowplow/snowplow-log-deserializers/blob/master/README.md
[recipesreadme]: https://github.com/snowplow/snowplow/blob/master/hive/recipes/README.md

[license]: http://www.apache.org/licenses/LICENSE-2.0
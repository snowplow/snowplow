# SnowPlow

## Introduction

SnowPlow is the world's most powerful web analytics platform. It does three things:

* Identifies users, and tracks the way they engage with a website or web-app
* Stores the associated data in a scalable “clickstream” data warehouse
* Makes it possible to leverage a big data toolset (e.g. Hadoop, Pig, Hive) to analyse that data

**To find out more, please check out the [SnowPlow website] [website] and the [SnowPlow wiki] [wiki].**

## SnowPlow technology 101

The repository structure follows the conceptual architecture of SnowPlow, which consists of five loosely coupled stages:

![architecture] [architecture-image]

To briefly explain these five sub-systems:

* **Trackers** fire SnowPlow events. Currently we have a JavaScript tracker; iOS and Android trackers are on the roadmap
* **Collectors** receive SnowPlow events from trackers. Currently we have a CloudFront-based collector and a node.js-based collector, called SnowCannon
* **ETL (extract, transform and load)** cleans up the raw SnowPlow events, enriches them and puts them into storage. Currently we have a Hive-based ETL process
* **Storage** is where the SnowPlow events live. Currently we store the SnowPlow events in a Hive-format flatfile structure on S3, and in the Infobright columnar database
* **Analytics** are performed on the SnowPlow events. Currently we have a set of ad hoc analyses that work with Hive and Infobright 

**For more information on the current SnowPlow architecture, please see the [Technical architecture] [architecture-doc]**.

## Documentation

1. The [SnowPlow setup guide] [setup] details how to choose between the different available trackers, collectors, ETL modules, storage solutions etc. and hwo to set each module up.
2. The [SnowPlow technical documentation] [tech-docs] provide technical details including the [SnowPlow tracker protocol] [tracker-protocol], [collector log file format schemas] [collector-logs] and [data structure schemas] [data-structure].

## Contributing

We're committed to a loosely-coupled architecture for SnowPlow and would love to get your contributions within each of the five sub-systems.

If you would like help implementing a new tracker, trying a different ETL approach or loading SnowPlow events into an alternative database, **[get in touch] [talk-to-us]**!

## Questions or need help?

Check out the **[Talk to us] [talk-to-us]** page on our wiki.

## Copyright and license

SnowPlow is copyright 2012 SnowPlow Analytics Ltd. Significant portions of `snowplow.js`
are copyright 2010 Anthon Pang.

Licensed under the **[Apache License, Version 2.0] [license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

![Tracker](https://collector.snplow.com/i?&e=pv&page=Root%20README&aid=snowplowgithub&p=web&tv=no-js-0.1.0)

[website]: http://snowplowanalytics.com
[wiki]: https://github.com/snowplow/snowplow/wiki
[architecture-image]: https://github.com/snowplow/snowplow/raw/master/architecture.png
[architecture-doc]: https://github.com/snowplow/snowplow/wiki/Technical-architecture
[talk-to-us]: https://github.com/snowplow/snowplow/wiki/Talk-to-us
[license]: http://www.apache.org/licenses/LICENSE-2.0
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[tech-docs]: https://github.com/snowplow/snowplow/wiki/SnowPlow%20technical%20documentation
[tracker-protocol]: https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol
[collector-logs]: https://github.com/snowplow/snowplow/wiki/Collector-logging-formats
[data-structure]: https://github.com/snowplow/snowplow/wiki/canonical-event-model

# Snowplow

## Introduction

Snowplow is the world's most powerful marketing and product analytics platform. It does three things:

* Identifies users, and tracks the way they engage with a website or app
* Stores the associated behavioural data in a scalable “clickstream” data warehouse
* Makes it possible to leverage a big data toolset (e.g. Hadoop, Pig, Hive) to analyse that event data

**To find out more, please check out the [Snowplow website] [website] and the [Snowplow wiki] [wiki].**

## Snowplow technology 101

The repository structure follows the conceptual architecture of Snowplow, which consists of five loosely coupled stages:

![architecture] [architecture-image]

To briefly explain these five sub-systems:

* **Trackers** fire Snowplow events. Currently we have a JavaScript tracker, a no-JavaScript (pixel) tracker and an Arduino tracker
* **Collectors** receive Snowplow events from trackers. Currently we have a CloudFront-based collector and a Clojure-based collector
* **Enrich** cleans up the raw Snowplow events, enriches them and puts them into storage. Currently we have separate Hadoop-based and Hive-based enrichment processes
* **Storage** is where the Snowplow events live. Currently we store the Snowplow events in a Hive-format flatfile structure on S3, and in the Redshift and Infobright columnar databases
* **Analytics** are performed on the Snowplow events. Currently we have a set of ad hoc analyses that work with Hive and Infobright 

**For more information on the current Snowplow architecture, please see the [Technical architecture] [architecture-doc]**.

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| **[Technical Docs] [techdocs]** | **[Setup Guide] [setup]** | **[Roadmap] [roadmap]** - **[Contributors] [contributors]** |

## Contributing

We're committed to a loosely-coupled architecture for Snowplow and would love to get your contributions within each of the five sub-systems.

If you would like help implementing a new tracker, adding an additional enrichment or loading Snowplow events into an alternative database, **[get in touch] [talk-to-us]**!

## Questions or need help?

Check out the **[Talk to us] [talk-to-us]** page on our wiki.

## Copyright and license

Snowplow is copyright 2012-2013 Snowplow Analytics Ltd.

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
[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/technical-architecture.png
[architecture-doc]: https://github.com/snowplow/snowplow/wiki/Technical-architecture
[talk-to-us]: https://github.com/snowplow/snowplow/wiki/Talk-to-us
[license]: http://www.apache.org/licenses/LICENSE-2.0
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[tech-docs]: https://github.com/snowplow/snowplow/wiki/SnowPlow%20technical%20documentation
[tracker-protocol]: https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol
[collector-logs]: https://github.com/snowplow/snowplow/wiki/Collector-logging-formats
[data-structure]: https://github.com/snowplow/snowplow/wiki/canonical-event-model
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[techdocs]: https://github.com/snowplow/snowplow/wiki/SnowPlow-technical-documentation
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[roadmap]: https://github.com/snowplow/snowplow/wiki/Product-roadmap
[contributors]: https://github.com/snowplow/snowplow/wiki/Contributors

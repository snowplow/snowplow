# Snowplow

[![License][license-image]][license]
[![Join the chat at https://gitter.im/snowplow/snowplow][gitter-image]][gitter]

<img src="https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-new-logo-large.png"
 alt="Snowplow logo" title="Snowplow" align="right" />

Snowplow is an enterprise-strength marketing and product analytics platform. It does three things:

1. Identifies your users, and tracks the way they engage with your website or application
2. Stores your users' behavioural data in a scalable "event data warehouse" you control: in Amazon S3 and (optionally) Amazon Redshift or Postgres
3. Lets you leverage the biggest range of tools to analyze that data, including big data tools (e.g. Spark) via EMR or more traditional tools e.g. Looker, Mode, Superset, Re:dash to analyze that behavioural data

**To find out more, please check out the [Snowplow website][website] and the [docs website][docs].**

## Snowplow technology 101

The repository structure follows the conceptual architecture of Snowplow, which consists of six loosely-coupled sub-systems connected by five standardized data protocols/formats:

![architecture][architecture-image]

To briefly explain these six sub-systems:

* **[Trackers][trackers]** fire Snowplow events. Currently we have 12 trackers, covering web, mobile, desktop, server and IoT
* **[Collector][collector]** receives Snowplow events from trackers. Currently we have one official collector implementation with different sinks: Apache Kafka, Amazon Kinesis, NSQ
* **[Enrich][enrich]** cleans up the raw Snowplow events, enriches them and puts them into storage. Currently we have several implementations, built for different environments (GCP, AWS, Apache Kafka) and one core library
* **[Storage][storage]** is where the Snowplow events live. Currently we store the Snowplow events in a flatfile structure on S3, and in the Redshift, Postgres, Snowflake and BigQuery databases
* **Data modeling** is where event-level data is joined with other data sets and aggregated into smaller data sets, and business logic is applied. This produces a clean set of tables which make it easier to perform analysis on the data. We have data models for Redshift and **[Looker][looker]**
* **Analytics** are performed on the Snowplow events or on the aggregate tables.

**For more information on the current Snowplow architecture, please see the [Technical architecture][architecture-doc]**.

## About this repository

This repository used to be an umbrella repository for all loosely-coupled Snowplow Components.
However, since June 2020 all components are extracted into their dedicated repositories and this repository serves as an entry point for OSS users and historical artifact.

## Find out more

| **[Technical Docs][techdocs]**     | **[Setup Guide][setup]**     | **[Roadmap][roadmap]**           | **[Contributing][contributing]**           |
|-------------------------------------|-------------------------------|-----------------------------------|---------------------------------------------|
| [![i1][techdocs-image]][techdocs] | [![i2][setup-image]][setup] | [![i3][roadmap-image]][roadmap] | [![i4][contributing-image]][contributing] |

## Contributing

We're committed to a loosely-coupled architecture for Snowplow and would love to get your contributions within each of the six sub-systems.

If you would like help implementing a new tracker, adding an additional enrichment or loading Snowplow events into an alternative database, check out our **[Contributing][contributing]** page on the wiki!

## Questions or need help?

Check out the **[Talk to us][talk-to-us]** page on our wiki.

## Copyright and license

Snowplow is copyright 2012-2019 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[gitter-image]: https://badges.gitter.im/snowplow/snowplow.svg
[gitter]: https://gitter.im/snowplow/snowplow?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

[trackers]: https://github.com/snowplow/snowplow/tree/master/1-trackers
[collector]: https://github.com/snowplow/stream-collector/
[enrich]: https://github.com/snowplow/enrich/
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage

[website]: https://snowplowanalytics.com
[docs]: https://docs.snowplowanalytics.com/
[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture.png
[architecture-doc]: https://github.com/snowplow/snowplow/wiki/Technical-architecture
[talk-to-us]: https://github.com/snowplow/snowplow/wiki/Talk-to-us
[contributing]: ./CONTRIBUTING.md

[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[tech-docs]: https://github.com/snowplow/snowplow/wiki/SnowPlow%20technical%20documentation
[tracker-protocol]: https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol
[collector-logs]: https://github.com/snowplow/snowplow/wiki/Collector-logging-formats
[data-structure]: https://github.com/snowplow/snowplow/wiki/canonical-event-model
[looker]: http://www.looker.com/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://github.com/snowplow/snowplow/wiki/SnowPlow-technical-documentation
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[roadmap]: https://github.com/snowplow/snowplow/wiki/Product-roadmap
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing

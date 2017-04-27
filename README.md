# Snowplow

[![Build Status][travis-image]][travis]
[![Release][release-image]][releases]
[![License][license-image]][license]

<img src="https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-logo-large.png"
 alt="Snowplow logo" title="Snowplow" align="right" />

Snowplow is an enterprise-strength marketing and product analytics platform. It does three things:

1. Identifies your users, and tracks the way they engage with your website or application
2. Stores your users' behavioural data in a scalable "event data warehouse" you control: in Amazon S3 and (optionally) Amazon Redshift or Postgres
3. Lets you leverage the biggest range of tools to analyze that data, including big data tools (e.g. Spark) via EMR or more traditional tools e.g. Looker, Mode, Caravel, Re:dash to analyze that behavioural data

**To find out more, please check out the [Snowplow website][website] and the [Snowplow wiki][wiki].**

## Snowplow technology 101

The repository structure follows the conceptual architecture of Snowplow, which consists of six loosely-coupled sub-systems connected by five standardized data protocols/formats:

![architecture][architecture-image]

To briefly explain these six sub-systems:

* **Trackers** fire Snowplow events. Currently we have 12 trackers, covering web, mobile, desktop, server and IoT
* **Collectors** receive Snowplow events from trackers. Currently we have three different event collectors, sinking events either to Amazon S3, Apache Kafka or Amazon Kinesis
* **Enrich** cleans up the raw Snowplow events, enriches them and puts them into storage. Currently we have a Hadoop-based enrichment process, and a Kinesis- or Kafka-based process
* **Storage** is where the Snowplow events live. Currently we store the Snowplow events in a flatfile structure on S3, and in the Redshift and Postgres databases
* **Data modeling** is where event-level data is joined with other data sets and aggregated into smaller data sets, and business logic is applied. This produces a clean set of tables which make it easier to perform analysis on the data. We have data models for Redshift and **[Looker][looker]**
* **Analytics** are performed on the Snowplow events or on the aggregate tables.

**For more information on the current Snowplow architecture, please see the [Technical architecture][architecture-doc]**.

## Quickstart

Assuming git, **[Vagrant][vagrant-install]** and **[VirtualBox][virtualbox-install]** installed:

```bash
 host$ git clone https://github.com/snowplow/snowplow.git
 host$ cd snowplow
 host$ vagrant up && vagrant ssh
guest$ cd /vagrant/3-enrich/scala-common-enrich
guest$ sbt test
```

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

Snowplow is copyright 2012-2017 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[travis-image]: https://travis-ci.org/snowplow/snowplow.png?branch=master
[travis]: http://travis-ci.org/snowplow/snowplow

[release-image]: https://img.shields.io/badge/release-88_Angkor_Wat-orange.svg?style=flat
[releases]: https://github.com/snowplow/snowplow/releases

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[website]: http://snowplowanalytics.com
[wiki]: https://github.com/snowplow/snowplow/wiki
[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture.png
[architecture-doc]: https://github.com/snowplow/snowplow/wiki/Technical-architecture
[talk-to-us]: https://github.com/snowplow/snowplow/wiki/Talk-to-us
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing

[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[tech-docs]: https://github.com/snowplow/snowplow/wiki/SnowPlow%20technical%20documentation
[tracker-protocol]: https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol
[collector-logs]: https://github.com/snowplow/snowplow/wiki/Collector-logging-formats
[data-structure]: https://github.com/snowplow/snowplow/wiki/canonical-event-model
[looker]: http://www.looker.com/

[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://github.com/snowplow/snowplow/wiki/SnowPlow-technical-documentation
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[roadmap]: https://github.com/snowplow/snowplow/wiki/Product-roadmap
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing

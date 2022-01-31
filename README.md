# Snowplow

[![Release][release-badge]][release]
[![License][license-image]][license]
[![Discourse posts][discourse-image]][discourse]

[![Snowplow logo][logo-image]][website]

## Overview

Snowplow is an enterprise-strength marketing and product analytics platform. It does three things:

1. Identifies your users, and tracks the way they engage with your website or application
2. Stores your users' behavioral data in a scalable "event data warehouse" you control: Amazon Redshift, Google BigQuery, Snowflake or Elasticsearch
3. Lets you leverage the biggest range of tools to analyze that data, including big data tools (e.g. Spark) via EMR or more traditional tools e.g. Looker, Mode, Superset, Re:dash to analyze that behavioral data

**To find out more, please check out the [Snowplow website][website] and the [docs website][docs].**

### Version Compatibility Matrix

For compatibility assurance, the version compatibility matrix offers clarity on our recommended stack. It is strongly recommended when setting up a Snowplow pipeline to use the versions listed in the version compatibility matrix which can be found [within our docs][version-compatibility].

### Public Roadmap

This repository also contains the [Snowplow Public Roadmap][roadmap]. The Public Roadmap lets you stay up to date and find out what's happening on the Snowplow Platform. Help us prioritize our cards: open the issue and leave a 👍 to vote for your favorites. Want us to build a feature or function? Tell us by heading to our [Discourse forum][discourse] 💬.

### Try Snowplow

Setting up a full open-source Snowplow pipeline requires a non-trivial amount of engineering expertise and time investment.
You might be interested in finding out what Snowplow can do first, by setting up [Try Snowplow][try-snowplow].

### Open Source Quick Start

The [Open Source Quick Start][open-source-quick-start] will help you get up and running with a Snowplow open source pipeline. Snowplow publishes a [set of terraform modules][terraform-modules], which automate the setting up & deployment of the required infrastructure & applications for an operational Snowplow open source pipeline, with just a handful of input variables required on your side.

### Join the Snowplow Research Panel and help shape the future of open source

As part of our ongoing efforts to improve the Snowplow Open Source experience, we're looking for users of our open-source software and  members of our community to take part in research studies. [Join here][research-survey].

### Our Commercial Offering

If you wish to get everything setup and managed for you, you can consider [Snowplow BDP][snowplow-bdp]. You can also [request a demo][request-a-demo].

## Snowplow technology 101

[![Snowplow architecture][architecture-image]][architecture]

The repository structure follows the conceptual architecture of Snowplow, which consists of six loosely-coupled sub-systems connected by five standardized data protocols/formats.

To briefly explain these six sub-systems:

* **[Trackers][trackers]** fire Snowplow events. Currently we have 15 trackers, covering web, mobile, desktop, server and IoT
* **[Collector][collector]** receives Snowplow events from trackers. Currently we have one official collector implementation with different sinks: Amazon Kinesis, Google PubSub, Amazon SQS, Apache Kafka and NSQ
* **[Enrich][enrich]** cleans up the raw Snowplow events, enriches them and puts them into storage. Currently we have several implementations, built for different environments (GCP, AWS, Apache Kafka) and one core library
* **[Storage][storage]** is where the Snowplow events live. Currently we store the Snowplow events in a flat file structure on S3, and in the Redshift, Postgres, Snowflake and BigQuery databases
* **[Data modeling][data-modeling]** is where event-level data is joined with other data sets and aggregated into smaller data sets, and business logic is applied. This produces a clean set of tables which make it easier to perform analysis on the data. We officially support data models for Redshift, Snowflake and BigQuery.
* **[Analytics][analytics-sdks]** are performed on the Snowplow events or on the aggregate tables.

**For more information on the current Snowplow architecture, please see the [Technical architecture][architecture]**.

## About this repository

This repository is an umbrella repository for all loosely-coupled Snowplow components and is updated on each component release.

Since June 2020, all components have been extracted into their dedicated repositories (more info [here][split-blogpost])
and this repository serves as an entry point for Snowplow users, the home of our public roadmap and as a historical artifact.

Components that have been extracted to their own repository are still here as [git submodules][submodules].

### Trackers

|                Web               |           Mobile           |         Gaming         |          TV          |       Desktop & Server        |
|:--------------------------------:|:--------------------------:|:----------------------:|:--------------------:|:-----------------------------:|
| [JavaScript][javascript-tracker] | [Android][android-tracker] | [Unity][unity-tracker] | [Roku][roku-tracker] | [Command line][tracking-cli]  |
| [AMP][amp-tracker]               | [iOS][ios-tracker]         |                        |                      | [.NET][dotnet-tracker]        |
|                                  | [React Native][rn-tracker] |                        |                      | [Go][golang-tracker]          |
|                                  | [Flutter][flutter-tracker] |                        |                      | [Java][java-tracker]          |
|                                  |                            |                        |                      | [Node.js][javascript-tracker] |
|                                  |                            |                        |                      | [PHP][php-tracker]            |
|                                  |                            |                        |                      | [Python][python-tracker]      |
|                                  |                            |                        |                      | [Ruby][ruby-tracker]          |
|                                  |                            |                        |                      | [Scala][scala-tracker]        |

### [Collector](https://github.com/snowplow/stream-collector)

### [Enrich](https://github.com/snowplow/enrich)

### Loaders

* [BigQuery (streaming)](https://github.com/snowplow-incubator/snowplow-bigquery-loader)
* [Redshift (batch)](https://github.com/snowplow/snowplow-rdb-loader)
* [Snowflake (batch)](https://github.com/snowplow-incubator/snowplow-snowflake-loader)
* [Google Cloud Storage (streaming)](https://github.com/snowplow-incubator/snowplow-google-cloud-storage-loader)
* [Amazon S3 (streaming)](https://github.com/snowplow/snowplow-s3-loader)
* [Postgres (streaming)](https://github.com/snowplow-incubator/snowplow-postgres-loader)
* [Elasticsearch (streaming)](https://github.com/snowplow/snowplow-elasticsearch-loader)

### Iglu

* [Iglu Server](https://github.com/snowplow-incubator/iglu-server/)
* [igluctl](https://github.com/snowplow-incubator/igluctl/)
* [Iglu Central](https://github.com/snowplow/iglu-central/)

### Data modeling

#### Web

* [Web model: SQL-Runner version](https://github.com/snowplow/data-models/tree/master/web/v1)
* [Web model: dbt version](https://github.com/snowplow/dbt-snowplow-web)

#### Mobile

* [Mobile model: SQL-Runner version](https://github.com/snowplow/data-models/tree/master/mobile/v1)

### Testing

* [Mini](https://github.com/snowplow/snowplow-mini)
* [Micro](https://github.com/snowplow-incubator/snowplow-micro)

### Parsing enriched event

* [Analytics SDK Scala](https://github.com/snowplow/snowplow-scala-analytics-sdk)
* [Analytics SDK Python](https://github.com/snowplow/snowplow-python-analytics-sdk)
* [Analytics SDK .NET](https://github.com/snowplow/snowplow-dotnet-analytics-sdk)
* [Analytics SDK Javascript](https://github.com/snowplow-incubator/snowplow-js-analytics-sdk/)
* [Analytics SDK Golang](https://github.com/snowplow/snowplow-golang-analytics-sdk)

### [Bad rows](https://github.com/snowplow-incubator/snowplow-badrows)

### [Terraform Modules][terraform-modules]

## Need help?

We want to make it super-easy for Snowplow users and contributors to talk to us and connect with each other, to share ideas, solve problems and help make Snowplow awesome. Here are the main channels we're running currently, we'd love to hear from you on one of them:

### [Discourse][discourse]

This is for all Snowplow users: engineers setting up Snowplow, data modelers structuring the data and data consumers building insights. You can find guides, recipes, questions and answers from Snowplow users including the Snowplow team.

We welcome all questions and contributions!

### Twitter

[@SnowplowData][snowplow-twitter] for official news or [@SnowplowLabs][snowplow-labs-twitter] for engineering-heavy conversations and release updates.

### GitHub

If you spot a bug, then please raise an issue in the GitHub repository of the component in question.
Likewise if you have developed a cool new feature or an improvement, please open a pull request,
we'll be glad to integrate it in the codebase!

If you want to brainstorm a potential new feature, then [Discourse][discourse] is the best place to start.

### Email

[community@snowplowanalytics.com][community-email]

If you want to talk directly to us (e.g. about a commercially sensitive issue), email is the easiest way.

## Copyright and license

Snowplow is copyright 2012-2022 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license-image]: https://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: https://www.apache.org/licenses/LICENSE-2.0

[logo-image]: media/snowplow_logo.png
[website]: https://snowplowanalytics.com
[docs]: https://docs.snowplowanalytics.com/open-source-docs/

[snowplow-bdp]: https://snowplowanalytics.com/products/snowplow-bdp/
[version-compatibility]: https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/version-compatibility-matrix/
[try-snowplow]: https://try.snowplowanalytics.com/?utm_source=github&utm_medium=post&utm_campaign=try-snowplow
[request-a-demo]: https://go.snowplowanalytics.com/l/571483/2021-05-04/3sv1pg8
[roadmap]: https://github.com/snowplow/snowplow/projects
[open-source-quick-start]: https://docs.snowplowanalytics.com/docs/open-source-quick-start/
[terraform-modules]: https://registry.terraform.io/modules/snowplow-devops
[research-survey]: https://forms.gle/pCtYx8naum7A8vvw5

[architecture-image]: media/snowplow_architecture.png
[architecture]: ./ARCHITECTURE.md

[trackers]: https://github.com/snowplow/snowplow/tree/master/1-trackers
[collector]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[data-modeling]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling
[analytics-sdks]: https://docs.snowplowanalytics.com/docs/modeling-your-data/analytics-sdk/

[split-blogpost]: https://snowplowanalytics.com/blog/2020/07/16/changing-releasing/
[submodules]: https://git-scm.com/book/en/v2/Git-Tools-Submodules

[discourse-image]: https://img.shields.io/discourse/posts?server=https%3A%2F%2Fdiscourse.snowplowanalytics.com%2F
[discourse]: http://discourse.snowplowanalytics.com/
[snowplow-twitter]: https://twitter.com/SnowplowData
[snowplow-labs-twitter]: https://twitter.com/SnowplowLabs
[community-email]: mailto:community@snowplowanalytics.com

[release]: https://github.com/snowplow/snowplow/releases/tag/21.08
[release-badge]: https://img.shields.io/badge/Snowplow-21.08%20North%20Cascades-6638b8

[javascript-tracker]: https://github.com/snowplow/snowplow-javascript-tracker
[amp-tracker]: https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/google-amp-tracker/
[android-tracker]: https://github.com/snowplow/snowplow-android-tracker
[ios-tracker]: https://github.com/snowplow/snowplow-objc-tracker
[rn-tracker]: https://github.com/snowplow-incubator/snowplow-react-native-tracker
[roku-tracker]: https://github.com/snowplow-incubator/snowplow-roku-tracker
[flutter-tracker]: https://github.com/snowplow-incubator/snowplow-flutter-tracker
[tracking-cli]: https://github.com/snowplow/snowplow-tracking-cli
[dotnet-tracker]: https://github.com/snowplow/snowplow-dotnet-tracker
[golang-tracker]: https://github.com/snowplow/snowplow-golang-tracker
[java-tracker]: https://github.com/snowplow/snowplow-java-tracker
[php-tracker]: https://github.com/snowplow/snowplow-php-tracker
[python-tracker]: https://github.com/snowplow/snowplow-python-tracker
[ruby-tracker]: https://github.com/snowplow/snowplow-ruby-tracker
[scala-tracker]: https://github.com/snowplow/snowplow-scala-tracker
[unity-tracker]: https://github.com/snowplow/snowplow-unity-tracker

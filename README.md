[![Snowplow logo][logo-image]][website]

[![Release][release-badge]][release]
![Release activity](https://img.shields.io/github/commit-activity/m/snowplow/snowplow?label=release%20activity)
![Latest release](https://img.shields.io/github/last-commit/snowplow/snowplow?label=latest%20release)
[![Docker pulls](https://img.shields.io/docker/pulls/snowplow/scala-stream-collector-kinesis)](https://hub.docker.com/r/snowplow/scala-stream-collector-kinesis/)
[![Discourse posts][discourse-image]][discourse]
[![License][license-image]][license]

---

As of January 8, 2024,  Snowplow is introducing the Snowplow Limited Use License Agreement, and we will be releasing new versions of our core behavioral data pipeline technology under this license.

Our mission to empower everyone to own their first-party customer behavioral data remains the same. We value all of our users and remain dedicated to helping our community use Snowplow in the optimal capacity that fits their business goals and needs. 

We reflect on our Snowplow origins and provide more information about these changes in our blog post here → https://eu1.hubs.ly/H06QJZw0

---

## Overview

Snowplow is a developer-first engine for collecting behavioral data. In short, it allows you to:

* Collect events such as impressions, clicks, video playback (or even custom events of your choosing).
* Store the data in a scalable data warehouse you control ([Amazon Redshift](https://aws.amazon.com/redshift/), [Databricks](https://databricks.com/product/databricks-sql), [Elasticsearch](https://www.elastic.co/), [Google BigQuery](https://cloud.google.com/bigquery), [Snowflake](https://www.snowflake.com/workloads/data-warehouse-modernization/)) or emit it via a stream ([Amazon Kinesis](https://aws.amazon.com/kinesis/), [Google PubSub](https://cloud.google.com/pubsub/docs/overview), [Kafka](https://kafka.apache.org/)).
* Leverage a wide range of tools to model and analyze the behavioral data: [dbt](https://www.getdbt.com/), [Looker](https://www.looker.com/), [Metabase](https://www.metabase.com/), [Mode](https://mode.com/), [Streamlit](https://streamlit.io/), [Superset](https://superset.apache.org/), [Redash](https://redash.io/), and more.

Thousands of organizations around the world generate, enhance, and model behavioral data with Snowplow to fuel [advanced analytics](https://snowplowanalytics.com/advanced-analytics/?utm_source=github&utm_content=main-repo), [AI/ML initiatives](https://snowplowanalytics.com/ai-ml/?utm_source=github&utm_content=main-repo), or [composable CDPs](https://snowplowanalytics.com/composable-cdp/?utm_source=github&utm_content=main-repo).

### Table of contents

* [Why Snowplow?](#why-snowplow)
* [Where to start?](#-where-to-start-%EF%B8%8F)
* [Snowplow technology 101](#snowplow-technology-101)
* [About this umbrella repository](#about-this-repository)
* [Community](#community)

### Why Snowplow?

* 🏔️ **Rock solid architecture** capable of processing billions of events per day.
* 🛠️ **Over [20 SDKs](https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/?utm_source=github&utm_content=main-repo)** to collect data from web, mobile, server-side, and other sources.
* ✅ A unique approach based on **[schemas and validation](https://docs.snowplowanalytics.com/docs/understanding-tracking-design/understanding-schemas-and-validation/?utm_source=github&utm_content=main-repo)** ensures your data is as clean as possible.
* 🪄 **Over [15 enrichments](https://docs.snowplowanalytics.com/docs/enriching-your-data/available-enrichments/?utm_source=github&utm_content=main-repo)** to get the most out of your data.
* 🏭 Send data to **popular warehouses and streams** — Snowplow fits nicely within the [Modern Data Stack](https://snowplowanalytics.com/blog/2021/05/12/modern-data-stack/?utm_source=github&utm_content=main-repo).

### ➡ Where to start? ⬅️

| [Snowplow Community Edition](https://snowplow.io/get-started/community-edition-signup/?utm_source=github&utm_content=landing-page)  | [Snowplow Behavioral Data Platform](https://snowplow.io/snowplow-bdp/?utm_source=github&utm_content=landing-page) |
| ------------- | ------------- |
| Community Edition equips you with everything you need to start creating behavioral data in a high-fidelity, machine-readable way. Head over to the [Quick Start Guide](https://docs.snowplow.io/docs/getting-started-on-community-edition/?utm_source=github&utm_content=landing-page) to set things up. | Looking for an enterprise solution with a console, APIs, data governance, workflow tooling? The Behavioral Data Platform is our managed service that runs in **your** AWS, Azure or GCP cloud. [Book a demo](https://snowplow.io/get-started/book-a-demo-of-snowplow-bdp/?utm_source=github&utm_content=landing-page). |

The [documentation](https://docs.snowplowanalytics.com/?utm_source=github&utm_content=main-repo) is a great place to learn more, especially:

* [Tracking design](https://docs.snowplowanalytics.com/docs/understanding-tracking-design/?utm_source=github&utm_content=main-repo) — discover how to approach creating your data the Snowplow way.
* [Pipelines](https://docs.snowplowanalytics.com/docs/understanding-your-pipeline/?utm_source=github&utm_content=main-repo) — understand what’s under the hood of Snowplow.

Would rather dive into the code? Then you are already in the right place!

---

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

---

## About this repository

This repository is an umbrella repository for all loosely-coupled Snowplow components and is updated on each component release.

Since June 2020, all components have been extracted into their dedicated repositories (more info [here][split-blogpost])
and this repository serves as an entry point for Snowplow users and as a historical artifact.

Components that have been extracted to their own repository are still here as [git submodules][submodules].

### Trackers

A full list of supported trackers can be found [on our documentation site](https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/). Popular trackers and use cases include:

|                Web               |           Mobile           |         Gaming         |          TV                |       Desktop & Server        |
|:--------------------------------:|:--------------------------:|:----------------------:|:--------------------------:|:-----------------------------:|
| [JavaScript][javascript-tracker] | [Android][android-tracker] | [Unity][unity-tracker] | [Roku][roku-tracker]       | [Command line][tracking-cli]  |
| [AMP][amp-tracker]               | [iOS][ios-tracker]         | [C++][cpp-tracker]     | [iOS][ios-tracker]         | [.NET][dotnet-tracker]        |
|                                  | [React Native][rn-tracker] | [Lua][lua-tracker]     | [Android][android-tracker] | [Go][golang-tracker]          |
|                                  | [Flutter][flutter-tracker] |                        | [React Native][rn-tracker] | [Java][java-tracker]          |
|                                  |                            |                        |                            | [Node.js][javascript-tracker] |
|                                  |                            |                        |                            | [PHP][php-tracker]            |
|                                  |                            |                        |                            | [Python][python-tracker]      |
|                                  |                            |                        |                            | [Ruby][ruby-tracker]          |
|                                  |                            |                        |                            | [Scala][scala-tracker]        |
|                                  |                            |                        |                            | [C++][cpp-tracker]            |
|                                  |                            |                        |                            | [Rust][rust-tracker]            |
|                                  |                            |                        |                            | [Lua][lua-tracker]            |

### [Collector](https://github.com/snowplow/stream-collector)

### [Enrich](https://github.com/snowplow/enrich)

### Loaders

* [Redshift (batch)](https://github.com/snowplow/snowplow-rdb-loader)
* [Snowflake (batch)](https://github.com/snowplow/snowplow-rdb-loader)
* [Databricks (batch)](https://github.com/snowplow/snowplow-rdb-loader)
* [BigQuery (streaming)](https://github.com/snowplow-incubator/snowplow-bigquery-loader)
* [Google Cloud Storage (streaming)](https://github.com/snowplow-incubator/snowplow-google-cloud-storage-loader)
* [Amazon S3 (streaming)](https://github.com/snowplow/snowplow-s3-loader)
* [Postgres (streaming)](https://github.com/snowplow-incubator/snowplow-postgres-loader)
* [Elasticsearch (streaming)](https://github.com/snowplow/snowplow-elasticsearch-loader)

### Iglu

* [Iglu Server](https://github.com/snowplow/iglu-server/)
* [igluctl](https://github.com/snowplow/igluctl/)
* [Iglu Central](https://github.com/snowplow/iglu-central/)

### Data modeling

#### Web

* [Web model: SQL-Runner version](https://github.com/snowplow/data-models/tree/master/web/v1)
* [Web model: dbt version](https://github.com/snowplow/dbt-snowplow-web)

#### Mobile

* [Mobile model: SQL-Runner version](https://github.com/snowplow/data-models/tree/master/mobile/v1)
* [Mobile model: dbt version](https://github.com/snowplow/dbt-snowplow-mobile)

#### Media

* [Media model: dbt version](https://github.com/snowplow/dbt-snowplow-media-player)

#### Retail

* [E-Commerce model: dbt version](https://github.com/snowplow/dbt-snowplow-ecommerce)

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

---

### Community 

We want to make it super easy for Snowplow users and contributors to talk to us and connect with one another, to share ideas, solve problems and help make Snowplow awesome. Join the conversation:

* **Meetups**. Don’t miss your chance to talk to us in person. We are often on the move with meetups in [Amsterdam](https://www.meetup.com/snowplow-analytics-amsterdam/), [Berlin](https://www.meetup.com/snowplow-analytics-berlin/), [Boston](https://www.meetup.com/snowplow-analytics-boston/), [London](https://www.meetup.com/snowplow-analytics-london/), and [more](https://www.meetup.com/topics/snowplow/all/).
* **Discourse**. [Our forum](http://discourse.snowplowanalytics.com/) for all Snowplow users: engineers setting up Snowplow, data modelers structuring the data, and data consumers building insights. You can find guides, recipes, questions and answers from Snowplow users and the Snowplow team. All questions and contributions are welcome!
* **Twitter**. Follow [@Snowplow](https://twitter.com/snowplow) for official news and [@SnowplowLabs](https://twitter.com/snowplowlabs) for engineering-heavy conversations and release announcements.
* **GitHub**. If you spot a bug, please raise an issue in the GitHub repository of the component in question. Likewise, if you have developed a cool new feature or an improvement, please open a pull request, we’ll be glad to integrate it in the codebase! For brainstorming a potential new feature, [Discourse](http://discourse.snowplowanalytics.com/) is the best place to start.
* **Email**. If you want to talk to Snowplow directly, email is the easiest way. Get in touch at community@snowplowanalytics.com.

---

### Copyright and license

Snowplow is copyright 2012-2023 Snowplow Analytics Ltd.

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
[terraform-modules]: https://registry.terraform.io/modules/snowplow-devops

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

[release]: https://github.com/snowplow/snowplow/releases/tag/22.01
[release-badge]: https://img.shields.io/badge/Snowplow-22.01%20Western%20Ghats-6638b8

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
[cpp-tracker]: https://github.com/snowplow/snowplow-cpp-tracker
[rust-tracker]: https://github.com/snowplow/snowplow-rust-tracker
[lua-tracker]: https://github.com/snowplow/snowplow-lua-tracker

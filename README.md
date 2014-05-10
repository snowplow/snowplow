# Snowplow

<img src="https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-logo-large.png"
 alt="Snowplow logo" title="Snowplow" align="right" />

Snowplow is an enterprise-strength marketing and product analytics platform. It does three things:

1. Identifies your users, and tracks the way they engage with your website or application
2. Stores your users' behavioural data in a scalable "event data warehouse" you control: in Amazon S3 and (optionally) Amazon Redshift or Postgres
3. Lets you leverage the biggest range of tools to analyse that data incl. big data toolset (e.g. Hive, Pig, Mahout) via EMR or more traditional tools e.g. Tableau, R, Chartio to analyse that behavioural data

**To find out more, please check out the [Snowplow website] [website] and the [Snowplow wiki] [wiki].**

## Snowplow technology 101

The repository structure follows the conceptual architecture of Snowplow, which consists of five loosely coupled stages:

![architecture] [architecture-image]

To briefly explain these five sub-systems:

* **Trackers** fire Snowplow events. Currently we have JavaScript, no-JavaScript (pixel), Lua and Arduino trackers
* **Collectors** receive Snowplow events from trackers. Currently we have a CloudFront-based collector and a Clojure-based collector
* **Enrich** cleans up the raw Snowplow events, enriches them and puts them into storage. Currently we have a Hadoop-based enrichment processes
* **Storage** is where the Snowplow events live. Currently we store the Snowplow events in a flatfile structure on S3, and in the Redshift and Postgres databases
* **Analytics** are performed on the Snowplow events. Currently we have a set of recipes and cubes as SQL views for both Redshift and Postgres, and an online cookbook of ad hoc analyses that work with Redshift, Postgres and Hive 

**For more information on the current Snowplow architecture, please see the [Technical architecture] [architecture-doc]**.

## Find out more

| **[Technical Docs] [techdocs]**     | **[Setup Guide] [setup]**     | **[Roadmap] [roadmap]**           | **[Contributing] [contributing]**           |
|-------------------------------------|-------------------------------|-----------------------------------|---------------------------------------------|
| [![i1] [techdocs-image]] [techdocs] | [![i2] [setup-image]] [setup] | [![i3] [roadmap-image]] [roadmap] | [![i4] [contributing-image]] [contributing] |

## Contributing

We're committed to a loosely-coupled architecture for Snowplow and would love to get your contributions within each of the five sub-systems.

If you would like help implementing a new tracker, adding an additional enrichment or loading Snowplow events into an alternative database, check out our **[Contributing] [contributing]** page on the wiki!

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

[website]: http://snowplowanalytics.com
[wiki]: https://github.com/snowplow/snowplow/wiki
[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/technical-architecture.png
[architecture-doc]: https://github.com/snowplow/snowplow/wiki/Technical-architecture
[talk-to-us]: https://github.com/snowplow/snowplow/wiki/Talk-to-us
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing
[license]: http://www.apache.org/licenses/LICENSE-2.0
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[tech-docs]: https://github.com/snowplow/snowplow/wiki/SnowPlow%20technical%20documentation
[tracker-protocol]: https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol
[collector-logs]: https://github.com/snowplow/snowplow/wiki/Collector-logging-formats
[data-structure]: https://github.com/snowplow/snowplow/wiki/canonical-event-model

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://github.com/snowplow/snowplow/wiki/SnowPlow-technical-documentation
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[roadmap]: https://github.com/snowplow/snowplow/wiki/Product-roadmap
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing

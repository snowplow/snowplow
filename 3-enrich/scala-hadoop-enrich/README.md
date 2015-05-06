# Scala Hadoop Enrichment

## Introduction

This is the Snowplow ETL process implemented for Hadoop using [Scalding] [scalding]. The Hadoop ETL parses raw Snowplow event logs, extracts the Snowplow events, enriches them (e.g. with geo-location information) and then writes them out to Snowplow-format event files.

The Snowplow Hadoop ETL process is an alternative to the Snowplow [Hive ETL] [hive-etl] process.

## Technical overview

The Snowplow Hadoop ETL process is written in [Scalding] [scalding], the Scala library/DSL on top of [Cascading] [cascading], the Java data processing framework which in turn wraps Hadoop.

Like the Hive ETL, the Hadoop ETL can be run on [Amazon Elastic MapReduce] [emr] using the [EmrEtlRunner] [emr-etl-runner] Ruby app.

## Building

Assuming you already have SBT installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 3-enrich/scala-hadoop-enrich
    $ sbt assembly

The 'fat jar' is now available as:

    target/snowplow-hadoop-etl-0.9.0.jar

## Testing

The `assembly` command above does not (currently) run the test suite - but you can run this manually with:

    $ sbt test

If you get errors reported in some tests, run those tests individually with `test-only com.snowplowanalytics...TestName` and they should pass fine.

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [techdocs] | [Setup Guide] [setup] | _coming soon_                        |

## Copyright and license

Copyright 2012-2014 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[scalding]: https://github.com/twitter/scalding/
[cascading]: http://www.cascading.org/
[snowplow]: http://snowplowanalytics.com
[hive-etl]: https://github.com/snowplow/snowplow/tree/master/3-enrich/hive-etl
[emr]: http://aws.amazon.com/elasticmapreduce/
[emr-etl-runner]: https://github.com/snowplow/snowplow/tree/master/3-enrich/emr-etl-runner

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[techdocs]: https://github.com/snowplow/snowplow/wiki/The-Enrichment-Process
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-EmrEtlRunner

[license]: http://www.apache.org/licenses/LICENSE-2.0

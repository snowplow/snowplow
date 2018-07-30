# Spark Enrich

## Introduction

This is the Snowplow Enrichment process implemented using [Spark][spark]. The Hadoop Enrichment parses raw Snowplow event logs, extracts the Snowplow events, enriches them (e.g. with geo-location information) and then writes them out to Snowplow-format event files.

The Snowplow Hadoop Enrichment process is an alternative to the Snowplow [Kinesis Enrichment][kinesis-enrich] process.

## Technical overview

Spark Enrich is written in [Spark][spark].

The process can be run on [Amazon Elastic MapReduce][emr] using the [EmrEtlRunner][emr-etl-runner] Ruby app.

## Building

Assuming you already have SBT installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 3-enrich/spark-enrich
    $ sbt assembly

The 'fat jar' is now available as:

    target/snowplow-spark-enrich-x.x.x.jar

## Testing

The `assembly` command above does not (currently) run the test suite - but you can run it manually with:

    $ sbt test

If you get errors reported in some tests, run those tests individually with `testOnly *TestName` and they should pass fine.

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

## Copyright and license

Copyright 2012-2017 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[spark]: http://spark.apache.org/
[snowplow]: http://snowplowanalytics.com
[kinesis-enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich/scala-kinesis-enrich
[emr]: http://aws.amazon.com/elasticmapreduce/
[emr-etl-runner]: https://github.com/snowplow/snowplow/tree/master/3-enrich/emr-etl-runner

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[techdocs]: https://github.com/snowplow/snowplow/wiki/The-Enrichment-Process
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-EmrEtlRunner

[license]: http://www.apache.org/licenses/LICENSE-2.0

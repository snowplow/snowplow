# SnowPlow Hadoop ETL

## Introduction

This is the SnowPlow ETL process implemented for Hadoop using [Scalding] [scalding].

The SnowPlow Hadoop ETL process is an alternative to the SnowPlow Hive ETL process. 

## Technical overview

The Hadoop ETL parses raw CloudFront log files, extracts the SnowPlow events, enriches them (e.g. with geo-location information) and then writes them out to SnowPlow-format flatfiles.

Like the Hive ETL, the Hadoop ETL can be run on [Amazon Elastic MapReduce] [emr] using the [EmrEtlRunner] [emr-etl-runner] Ruby app.

## Building

Assuming you already have SBT installed:

    $ git clone git://github.com/snowplow/snowplow.git
    $ cd 3-etl/hadoop-etl
    $ sbt assembly

The 'fat jar' is now available as:

    target/snowplow-hadoop-etl-0.0.1-fat.jar

## Unit testing

The `assembly` command above runs the test suite - but you can also run this manually with:

    $ sbt test
    <snip>
    [info] + A WordCount job should
	[info]   + count words correctly
	[info] Passed: : Total 2, Failed 0, Errors 0, Passed 2, Skipped 0

## Copyright and license

Copyright 2012 SnowPlow Analytics Ltd, with significant portions copyright 2012 Twitter, Inc.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[scalding]: https://github.com/twitter/scalding/
[snowplow]: http://snowplowanalytics.com
[emr]: http://aws.amazon.com/elasticmapreduce/
[emr-etl-runner]: https://github.com/snowplow/snowplow/tree/feature/scalding-etl/3-etl/emr-etl-runner
[license]: http://www.apache.org/licenses/LICENSE-2.0
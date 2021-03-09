# Event Manifest Populator

## Introduction

This is an [Apache Spark][spark] job to backpopulate a Snowplow event manifest in DynamoDB with the metadata of some or all enriched events from your archive in S3.
This one-off job solves the "cold start" problem for identifying cross-batch natural deduplicates in Snowplow's [Hadoop Shred step][shredding].

## Documentation

| Technical Docs             |
|----------------------------|
| ![i1][techdocs-image]      |
| [Technical Docs][techdocs] |
|                            |


## Building

Assuming [SBT](https://www.scala-sbt.org/) installed:

```bash
$ cd 5-data-modeling/event-manifest-populator
$ sbt assembly
```

## Copyright and License

Copyright 2017 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[spark]: http://spark.apache.org/
[shredding]: https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-rdb-loader/rdb-shredder/
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[techdocs]: https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/loaders-storage-targets/events-manifest-populator/
[license]: http://www.apache.org/licenses/LICENSE-2.0

# Enrich

![architecture][architecture-image]

## Overview

The **Enrich** process takes raw Snowplow events logged by a [Collector][collectors], cleans them up, enriches them and puts them into [Storage][storage].

## Available enrich

| ETL                       | Description                                                                  | Status                  |
|---------------------------|------------------------------------------------------------------------------|-------------------------|
| [stream-enrich][e3] (1)   | The Snowplow Enrichment process built as an Amazon Kinesis application       | Production-ready        |
| [beam-enrich][e6] (2)     | The Snowplow Enrichment process built as a Dataflow job on GCP               | Production-ready        |
| [spark-enrich][e1] (1)    | The Snowplow Enrichment process built using Apache Spark                     | Not actively maintained |
| [scala-common-enrich][e4] | A shared library for processing raw Snowplow events, used in (1) (2) and (3) | Production-ready        |
| [emr-etl-runner][e5]      | A Ruby app for running (1) and (2) on Amazon Elastic MapReduce               | Production-ready        |


## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-3-enrichment.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[e1]: https://github.com/snowplow/spark-enrich
[e3]: https://github.com/snowplow/enrich/tree/master/modules/stream
[e4]: https://github.com/snowplow/enrich/tree/master/modules/common
[e5]: https://github.com/snowplow/emr-etl-runner
[e6]: https://github.com/snowplow/enrich/tree/master/modules/beam
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-EmrEtlRunner
[techdocs]: https://github.com/snowplow/snowplow/wiki/Enrichment
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png

# Enrich

![architecture][architecture-image]

## Overview

The **Enrich** process takes raw Snowplow events logged by a [Collector][collectors], cleans them up, enriches them and puts them into [Storage][storage].

## Available enrich

| ETL                             | Description                                                              | Status           |
|---------------------------------|--------------------------------------------------------------------------|------------------|
| [scala-hadoop-enrich][e1] (1)  | The Snowplow Enrichment process built using Scalding for Apache Hadoop   | Production-ready |
| [scala-hadoop-shred][e2] (2)   | The Snowplow Shred process for shredding JSONs for loading into Redshift | Beta             |
| [hadoop-event-recovery][e3]    | A Hadoop job for retrieving raw Snowplow events from "bad bucket" errors | Production-ready |
| [stream-enrich][e4] (3)        | The Snowplow Enrichment process built as an Amazon Kinesis application   | Production-ready |
| [scala-common-enrich][e5]      | A shared library for processing raw Snowplow events, used in (1) and (3) | Production-ready |
| [emr-etl-runner][e6]           | A Ruby app for running (1) and (2) on Amazon Elastic MapReduce           | Production-ready |

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-3-enrichment.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[e1]: ./scala-hadoop-enrich/
[e2]: ./scala-hadoop-shred/
[e3]: ./hadoop-event-recovery/
[e4]: ./stream-enrich/
[e5]: ./scala-common-enrich/
[e6]: ./emr-etl-runner/
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-EmrEtlRunner
[techdocs]: https://github.com/snowplow/snowplow/wiki/Enrichment
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png

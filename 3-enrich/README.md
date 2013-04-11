# Enrich

![architecture] [architecture-image]

## Overview

The **Enrich** process takes raw Snowplow events logged by a [Collector] [collectors], cleans them up, enriches them and puts them into [Storage] [storage].

## Available enrich

| ETL                             | Description                                                  | Status           |
|---------------------------------|--------------------------------------------------------------|------------------|
| [hive-etl] [e1] (1)             | An ETL process built using Apache Hive                       | Production-ready |
| [hadoop-etl] [e2] (2)           | An ETL process built using Scalding for Apache Hadoop        | In beta          |
| [emr-etl-runner] [e3]           | A RubyGem for running (1) or (2) on Amazon Elastic MapReduce | Production-ready |

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [techdocs] | [Setup Guide] [setup] | _coming soon_                        |

![Tracker](https://collector.snplow.com/i?&e=pv&page=3%20ETL%20README&aid=snowplowgithub&p=web&tv=no-js-0.1.0)

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/3-enrich.png
[collectors]: https://github.com/snowplow/snowplow/tree/master/2-collectors
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[e1]: ./hive-etl/
[e2]: ./hadoop-etl/
[e3]: ./emr-etl-runner/
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-EmrEtlRunner
[techdocs]: https://github.com/snowplow/snowplow/wiki/etl
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png



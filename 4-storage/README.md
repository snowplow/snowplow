# Storage

![architecture][architecture-image]

## Overview

**Storage** is where atomic Snowplow events are stored by the [Enrich][enrich] process, ready for querying by Snowplow [Analytics][analytics] tools.

## Available storage

| Storage                       | Description                                                               | Status           |
|-------------------------------|---------------------------------------------------------------------------|------------------|
| [s3 / hive-storage][s1]      | Snowplow events stored in a Hive-compatible flatfile format on Amazon S3  | Production-ready |
| [redshift-storage][s3] (1)   | Snowplow events stored in a table in [Amazon Redshift][redshift]         | Production-ready |
| [postgres-storage][s2] (2)   | Snowplow events stored in a table in [PostgreSQL][postgres]              | Production-ready |
| [storage-loader][s4]         | A Ruby application for loading Snowplow events into (1) and (2)           | Production-ready |
| [kinesis-elasticsearch-sink][s5] | Snowplow events stored in [Elasticsearch][elasticsearch]             | Production-ready |

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-4-storage.png
[trackers]: https://github.com/snowplow/snowplow/tree/master/1-trackers
[enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich
[analytics]: https://github.com/snowplow/snowplow/tree/master/5-analytics
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-alternative-data-stores
[techdocs]: https://github.com/snowplow/snowplow/wiki/storage%20documentation

[redshift]: http://aws.amazon.com/redshift/
[postgres]: http://www.postgresql.org/
[elasticsearch]: http://www.elasticsearch.org/

[s1]: https://github.com/snowplow/snowplow/tree/master/4-storage/hive-storage
[s2]: https://github.com/snowplow/snowplow/tree/master/4-storage/postgres-storage
[s3]: https://github.com/snowplow/snowplow/tree/master/4-storage/redshift-storage
[s4]: https://github.com/snowplow/snowplow/tree/master/4-storage/storage-loader
[s5]: https://github.com/snowplow/snowplow/tree/master/4-storage/kinesis-elasticsearch-sink

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png

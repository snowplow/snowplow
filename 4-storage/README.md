# Storage

![architecture][architecture-image]

## Overview

**Storage** is where atomic Snowplow events are stored by the [Enrich][enrich] process, ready for querying by Snowplow [Analytics][analytics] tools.

## Available storage

| Storage                             | Description                                                               | Status           |
|-------------------------------------|-------------------------------------------------------------------------- |------------------|
| [hive-storage][s1]                  | Snowplow events stored in a Hive-compatible flatfile format on Amazon S3  | Deprecated       |
| [redshift-storage][s3] (1)          | Snowplow events stored in a table in [Amazon Redshift][redshift]          | Production-ready |
| [postgres-storage][s2] (2)          | Snowplow events stored in a table in [PostgreSQL][postgres]               | Production-ready |
| [rdb-loader][s4]                    | An EMR Step for loading Snowplow events into (1) and (2)                  | Production-ready |
| [snowplow-elasticsearch-loader][s5] | Snowplow events stored in [Elasticsearch][elasticsearch]                  | Production-ready |
| [snowplow-s3-loader][s6]            | Snowplow events stored on Amazon S3 real-time                             | Production-ready |
| [snowplow-bigquery-loader][s7]      | Snowplow events stored in GCP BigQuery real-time                          | Production-ready |
| [snowplow-snowflake-loader][s8]     | Snowplow events stored in Snowflake                                       | Production-ready |

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
[s4]: https://github.com/snowplow/snowplow-rdb-loader
[s5]: https://github.com/snowplow/snowplow-elasticsearch-loader
[s6]: https://github.com/snowplow/snowplow-s3-loader
[s7]: https://github.com/snowplow/snowplow-bigquery-loader
[s8]: https://github.com/snowplow/snowplow-snowflake-loader

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png

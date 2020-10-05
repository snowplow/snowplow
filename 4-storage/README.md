# Storage

![architecture][architecture-image]

**Storage** phase is where atomic Snowplow events emitted by [enrich][enrich] make their way to a data warehouse, for later processing.

## Loader

Storage of enriched events / bad rows happen thanks to a loader :

- [BigQuery (streaming)](https://github.com/snowplow-incubator/snowplow-bigquery-loader)
- [Redshift / Postgres (batch)](https://github.com/snowplow/snowplow-rdb-loader)
- [Snowflake (batch)](https://github.com/snowplow-incubator/snowplow-snowflake-loader)
- [Google Cloud Storage (streaming)](https://github.com/snowplow-incubator/snowplow-google-cloud-storage-loader)
- [Amazon S3 (streaming)](https://github.com/snowplow/snowplow-s3-loader)
- [Postgres (streaming)](https://github.com/snowplow-incubator/snowplow-postgres-loader)
- [Elasticsearch (streaming)](https://github.com/snowplow/snowplow-elasticsearch-loader)

## Table definitions

For [Postgres][postgres-definition] and [Redshidt][redshift-definition], tables need to be created beforehand. 

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-4-storage.png

[enrich]: https://github.com/snowplow/enrich

[postgres-definition]: https://github.com/snowplow/snowplow/tree/master/4-storage/postgres-storage
[redshift-definition]: https://github.com/snowplow/snowplow/tree/master/4-storage/redshift-storage
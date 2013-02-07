# Storage

![architecture] [architecture-image]

## Overview

**Storage** is where atomic SnowPlow events are stored by the [ETL] [etl] process, ready for querying by SnowPlow [Analytics] [analytics] tools.

## Available storage

| Storage                       | Description                                                               | Status           | Read more                    |
|-------------------------------|---------------------------------------------------------------------------|------------------|------------------------------|
| [s3 / hive-storage] [s1]      | SnowPlow events stored in a Hive-compatible flatfile format on Amazon S3  | Production-ready | [Wiki] [hive-on-wiki]        |
| [infobright-storage] [s2] (1) | SnowPlow events stored in a table in [Infobright Community Edition] [ice] | Production-ready | [Wiki] [ice-on-wiki]         |  
| [storage-loader] [s3]         | A Ruby application for loading SnowPlow events into (1)                   | Production-ready | [Wiki] [storage-loader-on-wiki] |  

## Documentation

1. The [Setup guide] [setup] includes a guide to installing [EmrEtlRunner] [setup-emretlrunner], which automatically loads SnowPlow data into S3 where it can be queried using Hive
2. It also includes a guide to [using the StorageLoader to load data into Infobright] [infobright-setup]
3. The [technical documentation] [tech-docs] includes a detailed overview of the [SnowPlow events model] [events-model] and table definitions for data stored in [Hive] [hive-table-def] and [Infobright] [infobright-table-def]

## Contributing

We welcome community contributions of new storage options for SnowPlow events! We have a guide to contributing storage options coming soon on the Wiki. 

![Tracker](https://collector.snplow.com/i?&e=pv&page=4%20%Storage%20README&aid=snowplowgithub&p=web&tv=no-js-0.1.0)

[architecture-image]: https://github.com/snowplow/snowplow/raw/master/4-storage/4-storage.png
[trackers]: https://github.com/snowplow/snowplow/tree/master/1-trackers
[etl]: https://github.com/snowplow/snowplow/tree/master/3-etl
[analytics]: https://github.com/snowplow/snowplow/tree/master/5-analytics
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-SnowPlow
[setup-emretlrunner]: https://github.com/snowplow/snowplow/wiki/setting-up-EmrEtlRunner
[infobright-setup]: https://github.com/snowplow/snowplow/wiki/setting-up-infobright
[tech-docs]: https://github.com/snowplow/snowplow/wiki/storage%20documentation
[events-model]: https://github.com/snowplow/snowplow/wiki/canonical-event-model
[hive-table-def]: https://github.com/snowplow/snowplow/wiki/s3-apache-hive-storage
[infobright-table-def]: https://github.com/snowplow/snowplow/wiki/infobright-storage


[s1]: https://github.com/snowplow/snowplow/tree/master/4-storage/hive-storage
[s2]: https://github.com/snowplow/snowplow/tree/master/4-storage/infobright-storage
[s3]: https://github.com/snowplow/snowplow/tree/master/4-storage/storage-loader
[ice]: http://www.infobright.org

[hive-on-wiki]: https://github.com/snowplow/snowplow/wiki/s3-hive-storage-setup
[ice-on-wiki]: https://github.com/snowplow/snowplow/wiki/infobright-storage-setup
[storage-loader-on-wiki]: https://github.com/snowplow/snowplow/wiki/StorageLoader-setup 

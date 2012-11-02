# Storage

![architecture] [architecture-image]

## Overview

**Storage** is where atomic SnowPlow events are stored by the [ETL] [etl] process, ready for querying by SnowPlow [Analytics] [analytics] tools.

## Available storage

| Storage                       | Description                                                               | Status           | Read more                    |
|-------------------------------|---------------------------------------------------------------------------|------------------|------------------------------|
| [hive-storage] [s1]           | SnowPlow events stored in a Hive-compatible flatfile format on Amazon S3  | Production-ready | Wiki                         |
| [infobright-storage] [s2] (1) | SnowPlow events stored in a table in [Infobright Community Edition] [ice] | Production-ready | Wiki                         |  
| [storage-loader] [s3]         | A Ruby application for loading SnowPlow events into (1)                   | Pre-alpha        | Coming soon                  |  

## Contributing

We welcome community contributions of new storage options for SnowPlow events! We have a guide to contributing storage options coming soon on the Wiki. 

[architecture-image]: https://github.com/snowplow/snowplow/raw/master/4-storage/4-storage.png
[trackers]: https://github.com/snowplow/snowplow/tree/master/1-trackers
[etl]: https://github.com/snowplow/snowplow/tree/master/3-etl
[analytics]: https://github.com/snowplow/snowplow/tree/master/5-analytics

[s1]: ./hive-storage/
[s2]: ./infobright-storage/
[s3]: ./storage-loader/
[ice]: http://www.infobright.org
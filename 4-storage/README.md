# Storage

![architecture] [architecture-image]

## Overview

**Storage** is where atomic Snowplow events are stored by the [Enrich] [enrich] process, ready for querying by Snowplow [Analytics] [analytics] tools.

## Available storage

| Storage                       | Description                                                               | Status           |
|-------------------------------|---------------------------------------------------------------------------|------------------|
| [s3 / hive-storage] [s1]      | Snowplow events stored in a Hive-compatible flatfile format on Amazon S3  | Production-ready |
| [redshift-storage] [s3] (1)   | Snowplow events stored in a table in [Amazon Redshift] [redshift]         | Production-ready |
| [infobright-storage] [s2] (2) | Snowplow events stored in a table in [Infobright Community Edition] [ice] | Production-ready |
| [storage-loader] [s4]         | A Ruby application for loading Snowplow events into (1) and (2)           | Production-ready |

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Technical Docs] [techdocs] | [Setup Guide] [setup] | _coming soon_                        |

![Tracker](https://collector.snplow.com/i?&e=pv&page=4%20Storage%20README&aid=snowplowgithub&p=web&tv=no-js-0.1.0)

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/4-storage.png
[trackers]: https://github.com/snowplow/snowplow/tree/master/1-trackers
[enrich]: https://github.com/snowplow/snowplow/tree/master/3-enrich
[analytics]: https://github.com/snowplow/snowplow/tree/master/5-analytics
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-alternative-data-stores
[techdocs]: https://github.com/snowplow/snowplow/wiki/storage%20documentation

[redshift]: http://aws.amazon.com/redshift/
[ice]: http://www.infobright.org

[s1]: https://github.com/snowplow/snowplow/tree/master/4-storage/hive-storage
[s2]: https://github.com/snowplow/snowplow/tree/master/4-storage/infobright-storage
[s3]: https://github.com/snowplow/snowplow/tree/master/4-storage/redshift-storage
[s4]: https://github.com/snowplow/snowplow/tree/master/4-storage/storage-loader

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
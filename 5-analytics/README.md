# Analytics

![architecture] [architecture-image]

## Overview

**Analytics** are performed on the Snowplow events held in [Storage] [storage], to answer business questions.

## Available analytics

| ETL                       | Description                                                     | Status           |
|---------------------------|-----------------------------------------------------------------|------------------|
| [looker-analytics] [e1]   | Snowplow meta model for analysis using [Looker] [looker]        | Production-ready |
| [postgres-analytics] [e2] | Cubes and recipes for Snowplow events stored in Postgres        | Production-ready |
| [redshift-analytics] [e3] | Cubes and recipes for Snowplow events stored in Amazon Redshift | Production-ready |

**See also: the [Snowplow Analytics Cookbook] [cookbook] on the website.**

## Documentation

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Analytics Cookbook] [cookbook] | [Setup Guide] [setup] | _coming soon_                        |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/5-analytics.png
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[setup]: https://github.com/snowplow/snowplow/wiki/getting-started-analysing-SnowPlow-data
[cookbook]: http://snowplowanalytics.com/analytics/index.html

[looker]: http://looker.com/

[e1]: ./looker-analytics/
[e2]: ./postgres-analytics/
[e3]: ./redshift-analytics/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png

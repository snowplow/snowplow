# Data modeling

![architecture] [architecture-image]

## Overview

In the previous [storage] [storage] step, your event-level data was loaded into your storage target.

Whilst it is possible to query this data directly, often it is more convenient to aggregate the event-level data up into a set of tables that are then used for querying. This has a number of advantages:

1. Basic business logic (e.g. identity stitching and sessionization) are applied to the data set, so that different users querying the data all end up using the same business logic. (And generating comparable results.)
2. The queries run tend to be much faster
3. It is easier to write queries
4. It is possible to connect a Business Intelligence or pivoting tool directly to the aggregate data

For more information on the data modeling stage in the funnel see the dedicated section in the [Analytics Cookbook] [cookbook].

## Available data models

Technically, there are four ways frameworks you can use to generate perform your data modeling, two of which are currently production-ready:

| Data modeling framework   | Description                                                     | Status           |
|---------------------------|-----------------------------------------------------------------|------------------|
| [sql-runner] [sql-runner]  | Express your aggregation logic in SQL, and apply this to the event-level data in your SQL-datawarehouseusing [sql-runner] [sql-runner-app]    | Production-ready |
| hadoop                    | Express your aggregation logic in Scalding or Cascalog, and apply this to the event-level data as part of the EMR enrichment process | To be developed |
| spark-streaming           | Express and run your aggregation logic using Spark streaming on Kinesis, as part of the real-time pipeline | To be developed |
| [looker] [looker]         | Aggregate your event-level data in LookML                       | Production-ready |

Note that the data models included are exemplar to get started: each Snowplow user will want to extend and customize the data models to incorporate their own business logic.

## Documentation

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |         
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Analytics Cookbook] [cookbook] | [Setup Guide] [setup] | _coming soon_                    |

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-5-data-modeling.png
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[setup]: https://github.com/snowplow/snowplow/wiki/getting-started-with-data-modeling
[cookbook]: http://snowplowanalytics.com/analytics/event-dictionaries-and-data-models/collection-enrichment-modeling-analysis.html#data-modeling

[e1]: ./looker-analytics/
[e2]: ./postgres-analytics/
[e3]: ./redshift-analytics/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png

[sql-runner]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling/sql-runner
[sql-runner-app]: https://github.com/snowplow/sql-runner
[looker]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling/looker

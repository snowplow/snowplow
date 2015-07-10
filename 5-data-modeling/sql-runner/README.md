# Data modeling using sql-runner

![architecture] [architecture-image]

## Overview

It is possible design your data modeling process in SQL and execute the SQL statements necessary to aggregate your data using the [sql-runner application] [sql-runner-app] as the last step in the data pipeline.

In this repo, we include several example data models (for web and mobile apps). The web models have the following business logic:

1. Users are identified via first party cookie IDs. (So this model will need to be extended to incorporate more sophisticated identity stitching and to work for events tracked from e.g. mobile platforms, where different user identifiers are captured.)
2. Sessionization is based on client-side Javascript (i.e. the domain_sessionidx)

Two different versions of this data model are included:

1. [Web-recalculate] [web-recalculate] computes the aggregate tables based on the complete data set in the datawarehouse.
2. [Web-incremental] [web-incremental] assumes that the data is loading into a staging schema (called `landing`). Then the aggregate tables are updated based only on the incremental data, before the event-level data is migrated out of the `landing` schema into the `atomic` schema.

## A guide to the contents of this section of the repo

| Section                   | Description                                                     |
|---------------------------|-----------------------------------------------------------------|
| [setup] [setup-section]   | Set of SQL queries for creating the tables that will be used in the data models |
| [sql] [sql-section]       | The actual SQL statements that make up the different data models |
| [playbooks] [playbooks-section] | The sql-runner YAML playbooks that you'll use in conjunction with [sql-runner] [sql-runner-app] |


## Documentation

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |
|-----------------------------|-----------------------|--------------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image]                |
| [Analytics Cookbook] [cookbook] | [Setup Guide] [setup] | _coming soon_                    |

[web-recalculate]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling/sql-runner/redshift/sql/web-recalculate
[web-incremental]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling/sql-runner/redshift/sql/web-incremental

[setup-section]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling/sql-runner/redshift/setup
[sql-section]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling/sql-runner/redshift/sql
[playbooks-section]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling/sql-runner/redshift/playbooks

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-5-data-modeling.png
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage
[setup]: https://github.com/snowplow/snowplow/wiki/getting-started-with-data-modeling
[cookbook]: http://snowplowanalytics.com/analytics/event-dictionaries-and-data-models/collection-enrichment-modeling-analysis.html#data-modeling

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png

[sql-runner]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling/sql-runner
[sql-runner-app]: https://github.com/snowplow/sql-runner
[looker]: https://github.com/snowplow/snowplow/tree/master/5-data-modeling/looker

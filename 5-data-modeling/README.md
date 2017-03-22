# Data modeling

![architecture][architecture-image]

## What is data modeling?

Once the [storage step][storage] is done loading enriched events into our storage target of choice, we can start to use that data to build intelligence.

Whilst it is possible to query the events directly, it is often more convenient to transform and aggregate the events into a set of tables that are then used by various data consumers within the business. This has a number of advantages:

1. It guarantees that all users within the business are using the same basic business logic (e.g. identity stitching and sessionization);
2. It is easier and faster to run queries against the modeled data
4. It is possible to connect a BI or pivoting tool directly to the aggregate data

## What data models are available?

### Web model

The web model is a starting point for Snowplow users to get up and running with the Snowplow web data. We expect that each Snowplow user will want to extend and customize the data model with their own unique business logic.

### Mobile model

Coming soon.

## Copyright and license

The data models are copyright 2016 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: http://www.apache.org/licenses/LICENSE-2.0
[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-5-data-modeling.png
[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage

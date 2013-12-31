# Scala Common Enrich

## Introduction

Scala Common Enrich is a shared library for processing raw Snowplow events into validated and enriched Snowplow events, ready for loading into Storage.

Scala Common Enrich provides record-level enrichment only: feeding in 1 raw Snowplow event will yield 0 or 1 records out, where a record may be an enriched Snowplow event or a reported bad record.

Scala Common Enrich is designed to be used within a "host" ETL process. The currently supported host ETL process is our Hadoop (Scalding/Cascading) ETL process; we are working on a new Amazon Kinesis-based host ETL process.

## Find out more

| Technical Docs              | Setup Guide           | Roadmap               | Contributing                  |
|-----------------------------|-----------------------|-----------------------|-------------------------------|
| ![i1] [techdocs-image]      | ![i2] [setup-image]   | ![i3] [roadmap-image] | ![i4] [contributing-image]    |
| [Technical Docs] [techdocs] | [Setup Guide] [setup] | [Roadmap] [roadmap]   | [Contributing] [contributing] |

## Copyright and license

Scala Common Enrich is copyright 2012-2014 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[]: 
[]: 

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: xxx
[setup]: xxx
[roadmap]: xxx
[contributing]: xxx

[license]: http://www.apache.org/licenses/LICENSE-2.0


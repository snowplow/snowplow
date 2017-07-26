# Relational Database Loader

## Introduction

This is a Scala application that runs as AWS EMR step, discovering [shredded data][shred] and loading it into one of possible [storage targets][targets].

Previously known as StorageLoader.


## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing   |         
|-----------------------------|-----------------------|--------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]       |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_              |

## Copyright and License

Snowplow Relational Database Loader is copyright 2012-2017 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[setup]: https://github.com/snowplow/snowplow/wiki/setting-up-EmrEtlRunner
[techdocs]: https://github.com/snowplow/snowplow/wiki/RDB_Loader

[targets]: https://github.com/snowplow/snowplow/wiki/Configuring-storage-targets
[shred]: https://github.com/snowplow/snowplow/wiki/Scala-Hadoop-Shred

[license]: http://www.apache.org/licenses/LICENSE-2.0

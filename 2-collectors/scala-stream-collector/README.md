# Scala Stream Collector

## Introduction

The Scala Stream Collector is an event collector for [Snowplow][snowplow], written in Scala.
It sets a third-party cookie, allowing user tracking across domains.

The Scala Stream Collector is designed to be easy to setup and store [Thrift][thrift] Snowplow events to [Amazon Kinesis][kinesis],
and is built on top of [spray][spray] and [akka][akka] Actors. 

## Find out more

| Technical Docs              | Setup Guide           | Roadmap & Contributing               |
|-----------------------------|-----------------------|--------------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]                |
| [Technical Docs][techdocs] | [Setup Guide][setup] | _coming soon_                        |

## Copyright and license

The Scala Stream Collector is copyright 2013-2014 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[snowplow]: http://snowplowanalytics.com

[thrift]: http://thrift.apache.org
[kinesis]: http://aws.amazon.com/kinesis
[spray]: http://spray.io/
[akka]: http://akka.io/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://github.com/snowplow/snowplow/wiki/Scala-Stream-collector
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-the-Scala-Stream-collector
[roadmap]: https://github.com/snowplow/snowplow/wiki/Scala-Stream-collector-roadmap
[contributing]: https://github.com/snowplow/snowplow/wiki/Scala-Stream-collector-contributing

[license]: http://www.apache.org/licenses/LICENSE-2.0

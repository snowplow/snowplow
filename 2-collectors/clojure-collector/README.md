# Clojure Collector

## Introduction

The Clojure Collector is a SnowPlow event collector for [SnowPlow] [snowplow], written in Clojure. It sets a third-party cookie, allowing user tracking across domains.

It is designed to be easily setup on [Amazon Elastic Beanstalk] [elastic-beanstalk].

The Clojure Collector relies on [Tomcat] [tomcat] for SnowPlow event logging, and is built on top of [Ring][ring] and [Compojure][compojure].

## Find out more

| Technical Documentation              | Setup Guide           | Roadmap & Contributing                 |         
|--------------------------------------|-----------------------|----------------------------------------|
| ![i1] [techdocs-image]               | ![i2] [setup-image]   | ![i3] [roadmap-image]                  |
| [Technical Documentation] [techdocs] | [Setup Guide] [setup] | Roadmap & Contributing (_coming soon_) |

## Copyright and license

The Clojure Collector is copyright 2012 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[snowplow]: http://snowplowanalytics.com
[elastic-beanstalk]: http://aws.amazon.com/elasticbeanstalk/
[tomcat]: http://tomcat.apache.org/
[ring]: https://github.com/ring-clojure/ring
[compojure]: https://github.com/weavejester/compojure

[techdocs-image]: https://github.com/snowplow/snowplow/raw/master/techdocs.png
[setup-image]: https://github.com/snowplow/snowplow/raw/master/setup.png
[roadmap-image]: https://github.com/snowplow/snowplow/raw/master/roadmap.png
[techdocs]: https://github.com/snowplow/snowplow/wiki/Clojure-collector
[setup]: https://github.com/snowplow/snowplow/wiki/Setting-up-the-Clojure-collector

[license]: http://www.apache.org/licenses/LICENSE-2.0

# SnowPlow

## Introduction

SnowPlow is the world's most powerful web analytics platform. It does three things:

* Identifies users, and tracks the way they engage with one or more websites
* Stores the associated data in a scalable “clickstream” data warehouse
* Makes it possible to leverage a big data toolset (e.g. Hadoop, Pig, Hive) to analyse that data

To find out more, read Keplar's blog post [introducing SnowPlow] [blogpost]. The rest of the
documentation in this repository focuses on the technical aspects of SnowPlow.

## Contents

Contents of this repository are as follows:

* The root folder contains this README and the [Apache License, Version 2.0] [license]
* `docs` contains the technical documentation for this project. See the next section for more details
* `tracker` contains everything related to SnowPlow's JavaScript tracker, `snowplow.js`

## Documentation

There is a growing set of technical documentation for SnowPlow:

* [Introduction] [intro]
* [Technical FAQ] [techfaq]
* [Integrating snowplow.js into your site] [integrating]
* [Self-hosting SnowPlow] [selfhosting]

Additionally there is a technical [README] [snowplowjs] for the `snowplow.js` JavaScript tracker within the `tracker` sub-folder.

## Roadmap

Planned items on the roadmap are as follows:

* Improvements to the JavaScript tracker, `snowplow.js`
* Opensourcing the standard SerDe
* Writing and opensourcing some standard Hive 'recipes'

## Contributors

* [Alex Dean](https://github.com/alexanderdean)
* [Yali Sassoon](https://github.com/yalisassoon)

Original concept for SnowPlow inspired by [Radek Maciaszek](https://github.com/rathko).

## Copyright and license

SnowPlow is copyright 2012 Orderly Ltd. Significant portions of `snowplow.js`
are copyright 2010 Anton Pang.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[blogpost]: http://www.keplarllp.com/blog/2012/02/introducing-snowplow-the-worlds-most-powerful-web-analytics-platform
[intro]: /snowplow/snowplow/blob/master/docs/01_introduction.md
[techfaq]: /snowplow/snowplow/blob/master/docs/02_technical_faq.md
[integrating]: /snowplow/snowplow/blob/master/docs/03_integrating_snowplowjs.md
[selfhosting]: /snowplow/snowplow/blob/master/docs/04_selfhosting_snowplow.md
[snowplowjs]: /snowplow/snowplow/blob/master/tracker/README.md
[license]: http://www.apache.org/licenses/LICENSE-2.0
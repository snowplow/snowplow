# SnowPlow

## Introduction

SnowPlow is the world's most powerful web analytics platform. It does three things:

* Identifies users, and tracks the way they engage with one or more websites
* Stores the associated data in a scalable “clickstream” data warehouse
* Makes it possible to leverage a big data toolset (e.g. Hadoop, Pig, Hive) to analyse that data

**To find out more, please check out the [SnowPlow wiki] [wiki].**

## Contents

Contents of this repository are as follows:

* The root folder contains this README, the CHANGELOG and the [Apache License, Version 2.0] [license]
* `tracker` contains everything related to SnowPlow's JavaScript tracker, `snowplow.js`
* `hive` contains everything related to running Hive analytics on the SnowPlow data

## Copyright and license

SnowPlow is copyright 2012 Orderly Ltd. Significant portions of `snowplow.js`
are copyright 2010 Anthon Pang.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[wiki]: https://github.com/snowplow/snowplow/wiki
[license]: http://www.apache.org/licenses/LICENSE-2.0
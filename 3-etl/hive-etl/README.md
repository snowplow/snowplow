# Hive ETL for SnowPlow

## Introduction

Hive is a very powerful tool for creating agile ETL processes on
web-scale data volumes. We have used Hive to build our initial ETL
process for SnowPlow data.

## Contents

The contents of this folder are as follows:

* In this folder is this README
* `hiveql` contains the Hive code to automate a regular ETL (extract-transform-load) job to process the daily SnowPlow log files
* `snowplow-log-deserializers` is an SBT project containing the deserializers to import SnowPlow logs into [Apache Hive] [hive] ready for analysis

## Copyright and license

SnowPlow is copyright 2012 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[analyst-cookbook]: https://github.com/snowplow/snowplow/wiki/Analysts-cookbook
[serdes]: https://github.com/snowplow/snowplow-log-deserializers
[hive]: http://hive.apache.org/
[serdereadme]: https://github.com/snowplow/snowplow-log-deserializers/blob/master/README.md
[license]: http://www.apache.org/licenses/LICENSE-2.0

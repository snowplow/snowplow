# Hive analytics for SnowPlow

## Introduction

Yali to write - why Hive is great for SnowPlow analytics.

## Contents

The contents of this folder are as follows:

* In this folder is this README and Apache 2.0 License
* `snowplow-log-deserializers` is a Git submodule (pointing to [this repository] [serdes]) containing the deserializers to import SnowPlow logs into [Apache Hive] [hive] ready for analysis
* `recipes` contains various HiveQL scripts to produce useful cuts and analyses of the SnowPlow data

## Documentation

Besides this README, we recommend reading:

* The [README] [serdereadme] for the SnowPlow Log Deserializers repository
* The [README] [recipereadme] detailing the different available HiveQL recipes for SnowPlow
* The guide to [Running a Hive Interactive Session] [hiveinteractive] 
* The [Introduction to the SnowPlow Hive Tables] [hivetables]

For a detailed set of tutorials on how to use Hive with SnowPlow for cohort analysis, please see the Keplar blog:

* [Cohort analyses for digital businesses: an overview] [cohort1]
* [Performing cohort analysis on web analytics data using SnowPlow] [cohort2]
* [Performing the cohort analysis described by Eric Ries in the Lean Startup] [cohort3]
* [On the wide variety of cohort analyses] [cohort4]
* [Approaches to measuring user engagement as part of cohort analysis] [cohort5]

## Roadmap

There is much left to be done on the Hive side of SnowPlow - if you would like to lend a hand, please [get in touch] [contact]! Roadmap tasks include:

* Publish additional HiveQL recipes for SnowPlow analysis
* Create a schedulable HiveQL job for collecting, deserializing and partitioning SnowPlow events on a daily basis 

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

[serdes]: https://github.com/snowplow/snowplow-log-deserializers
[hive]: http://hive.apache.org/

[serdereadme]: https://github.com/snowplow/snowplow-log-deserializers/blob/master/README.md
[recipereadme]: https://github.com/snowplow/snowplow/blob/master/hive/recipes/README.md
[hiveinteractive]: https://github.com/snowplow/snowplow/blob/master/docs/06_running_hive_interactive_session.md
[hivetables]: https://github.com/snowplow/snowplow/blob/master/docs/07_snowplow_hive_tables_introduction.md

[cohort1]: http://www.keplarllp.com/blog/2012/04/cohort-analyses-for-digital-businesses-an-overview
[cohort2]: http://www.keplarllp.com/blog/2012/05/performing-cohort-analysis-on-web-analytics-data-using-snowplow
[cohort3]: http://www.keplarllp.com/blog/2012/05/performing-the-cohort-analysis-described-in-eric-riess-lean-startup-using-snowplow-and-hive
[cohort4]: http://www.keplarllp.com/blog/2012/05/on-the-wide-variety-of-different-cohort-analyses-possible-with-snowplow
[cohort5]: http://www.keplarllp.com/blog/2012/05/different-approaches-to-measuring-user-engagement-with-snowplow

[contact]: snowplow@keplarllp.com
[license]: http://www.apache.org/licenses/LICENSE-2.0
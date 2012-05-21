# Hive analytics for SnowPlow

## Introduction

SnowPlow was built to meet the following 2 requirements:

1. Make it easy for website owners to capture granular, event-level and customer-level data capturing how their audience / customers engage on their websites and digital platforms and store that data securely
2. Make it possible to run a wide range of analytics across that entire data set including highly bespoke queries: thereby enabling website owners to answer very specific business questions using their data

Utilising [Apache Hive](http://hive.apache.org/) has been critical in our delivering on our 2nd objective: giving website owners a powerful, flexible tool to query data that is straightforward to use.

### Hive is powerful

Because it is built on top of Hadoop, Hive queries can be run against vast data sets. 

### Hive is flexible

Hive is a very flexible tool to use to query data.

1. It includes a large (and growing) number of inbuilt functions
2. Where Hive functions do not already exist to perform required operations, it is possible to create new user-defined functions
3. It is possible to extend Hive functionality using other languages including e.g. Python

### Hive is straightforward, especially as implemented for SnowPlow

Hive makes it easy for anyone with a knowledge of SQL to run queries against SnowPlow web analytics data. No knowledge of Java (or any other languages e.g. Python) is required.

SnowPlow data is stored in Hive in a single events table where each line of the table represents a single "event" or "user action" e.g. opening a web page, adding an item to a shopping basket, filling in an entry in a web form,  playing a video.

SnowPlow's SQL-like querying language combined with SnowPlow's simple data structure means that writing queries is very easy. To give just some examples:

#### Count the number of unique visits in a day

	SELECT COUNT(DISTINCT `user_id`)
	FROM `snowplow_events_table`
	WHERE `dt`='2012-05-20'

#### Count the average number of pages-per-visit by day

	SELECT COUNT(`tm`)/COUNT(DISTINCT `visit_id`) /* Average pages per visit */
	FROM `snowplow_events_table`
	WHERE `event_action` IS NULL /* i.e. ignore any ajax events */ 
	GROUP BY `dt` /* group by date */
	
#### Look at the number of visitors brought to the site by referrer for 1st visits ONLY

	SELECT
		`mkt_source`,
		`mkt_medium`,
		`mkt_term`,
		`mkt_content`,
		`mkt_name`,
		COUNT(DISTINCT (user_id)),
	FROM
		`snowplow_events_table`
	WHERE
		`visit_id` = ` /* Only look at 1st visits for each user_id */
	GROUP BY `mkt_source`, `mkt_medium`, `mkt_term`, `mkt_content`, `mkt_name`

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

## Tutorial: Advanced SnowPlow analytics with Hive

For a detailed tutorial series exploring how to use Hive with SnowPlow for advanced cohort analysis, please see the Keplar blog:

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
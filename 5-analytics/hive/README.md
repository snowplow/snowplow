# Hive analytics for SnowPlow

## Introduction

Hive is a very powerful tool for querying SnowPlow data.
An [Analyst's Cookbook] [analyst-cookbook] of Hive "recipes" is provided on the SnowPlow Analytics website: it provides a growing list of techniques and queries to use Hive to interrogate SnowPlow data.

## Example queries

Here are some example Hive queries which can be run on the SnowPlow data:

### Count the number of unique visits in a day

	SELECT COUNT(DISTINCT `user_id`)
	FROM `snowplow_events_table`
	WHERE `dt`='2012-05-20'

### Count the average number of pages-per-visit by day

	SELECT COUNT(`tm`)/COUNT(DISTINCT `visit_id`) /* Average pages per visit */
	FROM `snowplow_events_table`
	WHERE `event_action` IS NULL /* i.e. ignore any ajax events */ 
	GROUP BY `dt` /* group by date */
	
### Look at the number of visitors brought to the site by referrer for 1st visits ONLY

	SELECT
		`mkt_source`,
		`mkt_medium`,
		`mkt_term`,
		`mkt_content`,
		`mkt_campaign`,
		COUNT(DISTINCT (user_id)),
	FROM
		`snowplow_events_table`
	WHERE
		`visit_id` = ` /* Only look at 1st visits for each user_id */
	GROUP BY `mkt_source`, `mkt_medium`, `mkt_term`, `mkt_content`, `mkt_name`

## Documentation

Besides this README, we recommend reading:

* The [Setup guide] [setup]. This details how to install the EMR command line tools, and use those tools to start developing Hive queries and running those queries against SnowPlow data
* The [SnowPlow Analyst's Cookbook] [analyst-cookbook], a growing list of techniques and queries for interrogating SnowPlow data using Hive

## Copyright and license

SnowPlow is copyright 2012 SnowPlow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[analyst-cookbook]: http://snowplowanalytics.com/analytics/index.html
[serdes]: https://github.com/snowplow/snowplow-log-deserializers
[hive]: http://hive.apache.org/
[serdereadme]: https://github.com/snowplow/snowplow-log-deserializers/blob/master/README.md
[license]: http://www.apache.org/licenses/LICENSE-2.0
[setup]: https://github.com/snowplow/snowplow/wiki/hive%20analytics%20setup
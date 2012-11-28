# Hive analytics for SnowPlow

## Introduction

Hive is a very powerful tool for querying SnowPlow data.
An [Analyst's Cookbook] [analyst-cookbook] of Hive "recipes" is provided on the SnowPlow Analytics website: it provides a growing list of techniques and queries to use Hive to interrogate SnowPlow data.

## Table definition

Before you start querying SnowPlow data in Hive, you have to define the table that contains it:

	CREATE EXTERNAL TABLE IF NOT EXISTS `events` (
	tm string,
	txn_id string,
	user_id string,
	user_ipaddress string,
	visit_id int,
	page_url string,
	page_title string,
	page_referrer string,
	mkt_source string,
	mkt_medium string,
	mkt_term string,
	mkt_content string,
	mkt_campaign string,
	ev_category string,
	ev_action string,
	ev_label string,
	ev_property string,
	ev_value string,
	tr_orderid string,
	tr_affiliation string,
	tr_total string,
	tr_tax string,
	tr_shipping string,
	tr_city string,
	tr_state string,
	tr_country string,
	ti_orderid string,
	ti_sku string,
	ti_name string,
	ti_category string,
	ti_price string,
	ti_quantity string,
	br_name string,
	br_family string,
	br_version string,
	br_type string,
	br_renderengine string,
	br_lang string,
	br_features array<string>,
	br_cookies boolean,
	os_name string,
	os_family string,
	os_manufacturer string,
	dvce_type string,
	dvce_ismobile boolean,
	dvce_screenwidth int,
	dvce_screenheight int,
	app_id string,
	platform string,
	event_name string,
	v_tracker string,
	v_collector string,
	v_etl string
	)
	PARTITIONED BY (dt STRING)
	LOCATION '${EVENTS_TABLE}' ;

Once defined, you have to tell Hive to `RECOVER PARTITIONS` i.e. scan S3 and identify all the partitions where data is already stored:

	ALTER TABLE `events` RECOVER PARTITIONS ;

## Example queries

Here are some example Hive queries which can be run on the SnowPlow data:

### Count the number of unique visits in a day

	SELECT COUNT(DISTINCT `user_id`)
	FROM `events`
	WHERE `dt`='2012-05-20'

### Count the average number of pages-per-visit by day

	SELECT COUNT(`tm`)/COUNT(DISTINCT `visit_id`) /* Average pages per visit */
	FROM `events`
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
		`events`
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
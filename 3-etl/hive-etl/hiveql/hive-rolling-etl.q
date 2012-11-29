-- Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
--
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
--
-- Version:     0.5.2
-- URL:         s3://snowplow-emr-assets/hive/hiveql/hive-rolling-etl-0.5.2.q
--
-- Authors:     Alex Dean, Yali Sassoon, Simon Andersson, Michael Tibben
-- Copyright:   Copyright (c) 2012 SnowPlow Analytics Ltd
-- License:     Apache License Version 2.0

SET hive.exec.dynamic.partition=true ;
SET hive.exec.dynamic.partition.mode=nonstrict ;

ADD JAR ${SERDE_FILE} ;

CREATE EXTERNAL TABLE `extracted_logs`
ROW FORMAT SERDE 'com.snowplowanalytics.snowplow.hadoop.hive.SnowPlowEventDeserializer'
WITH SERDEPROPERTIES ( 'continue_on_unexpected_error' = '${CONTINUE_ON}')
LOCATION '${CLOUDFRONT_LOGS}' ;

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
event string,
v_tracker string,
v_collector string,
v_etl string,
event_id string
user_fingerprint string,
useragent string,
br_colordepth string,
os_timezone string
)
PARTITIONED BY (dt STRING)
LOCATION '${EVENTS_TABLE}' ;

ALTER TABLE events RECOVER PARTITIONS ;

INSERT INTO TABLE `events`
PARTITION (dt)
SELECT
tm,
txn_id,
user_id,
user_ipaddress,
visit_id,
page_url,
page_title,
page_referrer,
mkt_source,
mkt_medium,
mkt_term,
mkt_content,
mkt_campaign,
ev_category,
ev_action,
ev_label,
ev_property,
ev_value,
tr_orderid,
tr_affiliation,
tr_total,
tr_tax,
tr_shipping,
tr_city,
tr_state,
tr_country,
ti_orderid,
ti_sku,
ti_name,
ti_category,
ti_price,
ti_quantity,
br_name,
br_family,
br_version,
br_type,
br_renderengine,
br_lang,
br_features,
br_cookies,
os_name,
os_family,
os_manufacturer,
dvce_type,
dvce_ismobile,
dvce_screenwidth,
dvce_screenheight,
app_id,
platform,
event, -- Now available in 0.5.2
v_tracker,
v_collector,
v_etl,
event_id, -- Now available in 0.5.2
user_fingerprint,
useragent,
br_colordepth,
os_timezone,
dt
FROM `extracted_logs` ;
-- Copyright (c) 2013 Snowplow Analytics Ltd. All rights reserved.
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
-- Version:     0.1.0
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean
-- Copyright:   Copyright (c) 2013-2014 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

CREATE EXTERNAL TABLE IF NOT EXISTS `events` (
app_id string,
platform string,
collector_tstamp timestamp,
dvce_tstamp timestamp,
event string,
event_vendor string,
event_id string,
txn_id int,
name_tracker string,               -- Added in 0.1.0
v_tracker string,
v_collector string,
v_etl string,
user_id string,
user_ipaddress string,
user_fingerprint string,
domain_userid string,
domain_sessionidx smallint,
network_userid string,
geo_country string,
geo_region string,
geo_city string,
geo_zipcode string,
geo_latitude double,
geo_longitude double,
page_url string,                   -- Added in 0.1.0
page_title string,
page_referrer string,              -- Added in 0.1.0
page_urlscheme string,
page_urlhost string,
page_urlport int, 
page_urlpath string,
page_urlquery string,
page_urlfragment string,
refr_urlscheme string,
refr_urlhost string,
refr_urlport int,
refr_urlpath string,
refr_urlquery string,
refr_urlfragment string,
refr_medium string,
refr_source string,
refr_term string,
mkt_medium string,
mkt_source string,
mkt_term string,
mkt_content string,
mkt_campaign string,
contexts string,                   -- Added in 0.1.0
se_category string,
se_action string,
se_label string,
se_property string,
se_value double,
ue_name string,                    -- Added in 0.1.0
ue_properties string,              -- Added in 0.1.0
tr_orderid string,
tr_affiliation string,
tr_total double,
tr_tax double,
tr_shipping double,
tr_city string,
tr_state string,
tr_country string,
ti_orderid string,
ti_sku string,
ti_name string,
ti_category string,
ti_price double,
ti_quantity int,
pp_xoffset_min int,
pp_xoffset_max int,
pp_yoffset_min int,
pp_yoffset_max int,
useragent string,
br_name string,
br_family string,
br_version string,
br_type string,
br_renderengine string,
br_lang string,
br_features_pdf boolean,
br_features_flash boolean,
br_features_java boolean,
br_features_director boolean,
br_features_quicktime boolean,
br_features_realplayer boolean,
br_features_windowsmedia boolean,
br_features_gears boolean ,
br_features_silverlight boolean,
br_cookies boolean,
br_colordepth string,
br_viewwidth int,
br_viewheight int,
os_name string,
os_family string,
os_manufacturer string,
os_timezone string,
dvce_type string,
dvce_ismobile boolean,
dvce_screenwidth int,
dvce_screenheight int,
doc_charset string,
doc_width int,
doc_height int
)
PARTITIONED BY (run string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${EVENTS_TABLE}' ;
-- Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
-- Version:     Ports version 0.0.6 (CloudFront collector) to version 0.0.7
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean
-- Copyright:   Copyright (c) 2012-2013 SnowPlow Analytics Ltd
-- License:     Apache License Version 2.0

-- Note: Infobright does not support MySQL-like ALTER TABLE statements
-- Approach below follows recommended best practice for ICE:
-- http://www.infobright.org/images/uploads/blogs/how-to/How_To_ALTER_TABLE_in_Infobright.pdf

USE snowplow ;

SELECT 
	-- App
	`app_id`,
	`platform`,
	-- Date/time
	`dt` AS `collector_dt`, -- Renamed in 0.0.7 (no action to migrate)
	`tm` AS `collector_tm`, -- Renamed in 0.0.7 (no action to migrate)
	null AS `dvce_dt`,      -- New in 0.0.7
	null AS `dvce_tm`,      -- New in 0.0.7
	null AS `dvce_epoch`,   -- New in 0.0.7
	-- Event
	`event`,
	`event_vendor`,
	`event_id`,
	`txn_id`,
	-- Versioning
	`v_tracker`,
	`v_collector`,
	`v_etl`,
	-- User and visit
	null AS `user_id`, -- Changed meaning in 0.0.7
	`user_ipaddress`,
	`user_fingerprint`,
	`user_id` AS `domain_userid`,      -- New in 0.0.7. If using CloudFront collector, user_ids are now domain_userids
	`visit_id` AS `domain_sessionidx`, -- Renamed in 0.0.7 (no action to migrate)
	null AS `network_userid`,          -- New in 0.0.7. Not set by CloudFront collector
	-- Page
	`page_url`,
	`page_title`,
	`page_referrer`,
	-- Page URL components
	`page_urlscheme`,
	`page_urlhost`,
	`page_urlport`,
	`page_urlpath`,
	`page_urlquery`,
	`page_urlfragment`,
	-- Marketing
	`mkt_source`,
	`mkt_medium`,
	`mkt_term`,
	`mkt_content`,
	`mkt_campaign`,
	-- Custom Event
	`ev_category`,
	`ev_action`,
	`ev_label`,
	`ev_property`,
	`ev_value`,
	-- Ecommerce
	`tr_orderid`,
	`tr_affiliation`,
	`tr_total`,
	`tr_tax`,
	`tr_shipping`,
	`tr_city`,
	`tr_state`,
	`tr_country`,
	`ti_orderid`,
	`ti_sku`,
	`ti_name`,
	`ti_category`,
	`ti_price`,
	`ti_quantity`,
	-- Page ping
	`pp_xoffset_min`,
	`pp_xoffset_max`,
	`pp_yoffset_min`,
	`pp_yoffset_max`,
	-- User Agent
	`useragent`,
	-- Browser
	`br_name`,
	`br_family`,
	`br_version`,
	`br_type`,
	`br_renderengine`,
	`br_lang`,
	`br_features_pdf`,
	`br_features_flash`,
	`br_features_java`,
	`br_features_director`,
	`br_features_quicktime`,
	`br_features_realplayer`,
	`br_features_windowsmedia`,
	`br_features_gears`,
	`br_features_silverlight`,
	`br_cookies`,
	`br_colordepth`,
	`br_viewwidth`,
	`br_viewheight`,
	-- Operating System
	`os_name`,
	`os_family`,
	`os_manufacturer`,
	`os_timezone`,
	-- Device/Hardware
	`dvce_type`,
	`dvce_ismobile`,
	`dvce_screenwidth`,
	`dvce_screenheight`,
	-- Document
	`doc_charset`,
	`doc_width`,
	`doc_height`
FROM events_006 INTO OUTFILE '/tmp/events_007'
FIELDS TERMINATED BY '|';

LOAD DATA INFILE '/tmp/events_007' INTO TABLE events_007
FIELDS TERMINATED BY '|';
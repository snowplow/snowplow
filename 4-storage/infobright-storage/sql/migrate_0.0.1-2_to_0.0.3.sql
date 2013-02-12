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
-- Version:     Ports versions 0.0.1 or 0.0.2 to version 0.0.3
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
	null AS `app_id`, -- 'lookup' is a varchar optimisation
	`platform`,
	-- Date/time
	`dt`,
	`tm`,
	-- Event
	`event_name`, -- Renamed in 0.0.3 to event
	null AS `event_id`, -- New in 0.0.3
	`txn_id`,
	-- Versioning
	`v_tracker`,
	`v_collector`,
	`v_etl`,
	-- User and visit
	`user_id`,
	`user_ipaddress`,
	null AS `user_fingerprint`, -- New in 0.0.3
	`visit_id`,
	-- Page
	`page_url`,
	`page_title`,
	`page_referrer`,
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
	-- User Agent
	null AS `useragent`, -- New in 0.0.3
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
	null AS `br_colordepth`, -- New in 0.0.3
	-- Operating System
	`os_name`,
	`os_family`,
	`os_manufacturer`,
	null AS `os_timezone`, -- New in 0.0.3
	-- Device/Hardware
	`dvce_type`,
	`dvce_ismobile`,
	`dvce_screenwidth`,
	`dvce_screenheight`
FROM events INTO OUTFILE '/tmp/events_003'
FIELDS TERMINATED BY '|';

LOAD DATA INFILE '/tmp/events_003' INTO TABLE events_003
FIELDS TERMINATED BY '|';
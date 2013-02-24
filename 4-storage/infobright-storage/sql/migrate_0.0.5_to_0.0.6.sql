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
-- Version:     Ports version 0.0.4 to version 0.0.6
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
	`dt`,
	`tm`,
	-- Event
	`event`,
	"com.snowplowanalytics" AS `event_vendor`, -- New in 0.0.6
	`event_id`,
	`txn_id`,
	-- Versioning
	`v_tracker`,
	`v_collector`,
	`v_etl`,
	-- User and visit
	`user_id`,
	`user_ipaddress`,
	`user_fingerprint`,
	`visit_id`,
	-- Page
	`page_url`,
	`page_title`,
	`page_referrer`,
	-- Page URL components
	null AS `page_urlscheme`,   -- New in 0.0.6
	null AS `page_urlhost`,     -- New in 0.0.6
	null AS `page_urlport`,     -- New in 0.0.6
	null AS `page_urlpath`,     -- New in 0.0.6
	null AS `page_urlquery`,    -- New in 0.0.6
	null AS `page_urlfragment`, -- New in 0.0.6
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
	null AS `pp_xoffset_min`, -- New in 0.0.6
	null AS `pp_xoffset_max`, -- New in 0.0.6
	null AS `pp_yoffset_min`, -- New in 0.0.6
	null AS `pp_yoffset_max`, -- New in 0.0.6
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
	null AS `br_viewwidth`,  -- New in 0.0.6
	null AS `br_viewheight`, -- New in 0.0.6
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
	null AS `doc_charset`, -- New in 0.0.6
	null AS `doc_width`, -- New in 0.0.6
	null AS `doc_height` -- New in 0.0.6
FROM events_005 INTO OUTFILE '/tmp/events_006'
FIELDS TERMINATED BY '|';

LOAD DATA INFILE '/tmp/events_006' INTO TABLE events_006
FIELDS TERMINATED BY '|';
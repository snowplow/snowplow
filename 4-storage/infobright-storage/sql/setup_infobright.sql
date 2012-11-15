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
-- Version:     0.0.3
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean
-- Copyright:   Copyright (c) 2012 SnowPlow Analytics Ltd
-- License:     Apache License Version 2.0

CREATE DATABASE IF NOT EXISTS snowplow ;

USE snowplow ;

CREATE TABLE IF NOT EXISTS events_003 (
	-- App
	`app_id` varchar(255) comment 'lookup', -- 'lookup' is a varchar optimisation
	`platform` varchar(50) comment 'lookup',
	-- Date/time
	`dt` date,
	`tm` time,
	-- Event
	`event` varchar(255) comment 'lookup', -- Renamed in 0.0.3
	`txn_id` int,
	-- Versioning
	`v_tracker` varchar(100) comment 'lookup',
	`v_collector` varchar(100) comment 'lookup',
	`v_etl` varchar(100) comment 'lookup',
	-- User and visit
	`user_id` varchar(16) comment 'lookup',
	`user_ipaddress` varchar(19) comment 'lookup',
	`user_fingerprint` varchar(50) comment 'lookup', -- New in 0.0.3
	`visit_id` smallint,
	-- Page
	`page_url` varchar(2083) comment 'lookup',
	`page_title` varchar(2083) comment 'lookup',
	`page_referrer` varchar(2083) comment 'lookup',
	-- Marketing
	`mkt_source` varchar(255) comment 'lookup',
	`mkt_medium` varchar(255) comment 'lookup',
	`mkt_term` varchar(255) comment 'lookup',
	`mkt_content` varchar(2083) comment 'lookup',
	`mkt_campaign` varchar(255) comment 'lookup',
	-- Custom Event
	`ev_category` varchar(255) comment 'lookup',
	`ev_action` varchar(255) comment 'lookup',
	`ev_label` varchar(255) comment 'lookup',
	`ev_property` varchar(255) comment 'lookup',
	`ev_value` float,
	-- Ecommerce
	`tr_orderid` varchar(255) comment 'lookup',
	`tr_affiliation` varchar(255),
	`tr_total` dec(18,2),
	`tr_tax` dec(18,2),
	`tr_shipping` dec(18,2),
	`tr_city` varchar(255) comment 'lookup',
	`tr_state` varchar(255) comment 'lookup',
	`tr_country` varchar(255) comment 'lookup',
	`ti_orderid` varchar(255) comment 'lookup',
	`ti_sku` varchar(255) comment 'lookup',
	`ti_name` varchar(255) comment 'lookup',
	`ti_category` varchar(255) comment 'lookup',
	`ti_price` dec(18,2),
	`ti_quantity` int,
	-- User Agent
	`useragent` (2083) comment 'lookup', -- New in 0.0.3
	-- Browser
	`br_name` varchar(50) comment 'lookup',
	`br_family` varchar(50) comment 'lookup',
	`br_version` varchar(50) comment 'lookup',
	`br_type` varchar(50) comment 'lookup',
	`br_renderengine` varchar(50) comment 'lookup',
	`br_lang` varchar(10) comment 'lookup',
	`br_features_pdf` tinyint(1),
	`br_features_flash` tinyint(1),
	`br_features_java` tinyint(1),
	`br_features_director` tinyint(1),
	`br_features_quicktime` tinyint(1),
	`br_features_realplayer` tinyint(1),
	`br_features_windowsmedia` tinyint(1),
	`br_features_gears` tinyint(1) ,
	`br_features_silverlight` tinyint(1),
	`br_cookies` tinyint(1),
	`br_colordepth` varchar(12) comment 'lookup', -- New in 0.0.3
	-- Operating System
	`os_name` varchar(50) comment 'lookup',
	`os_family` varchar(50) comment 'lookup',
	`os_manufacturer` varchar(50) comment 'lookup',
	`os_timezone` varchar(255) comment 'lookup', -- New in 0.0.3
	-- Device/Hardware
	`dvce_type` varchar(50) comment 'lookup',
	`dvce_ismobile` tinyint(1),
	`dvce_screenwidth` mediumint,
	`dvce_screenheight` mediumint
) ENGINE=BRIGHTHOUSE DEFAULT CHARSET=utf8 ;

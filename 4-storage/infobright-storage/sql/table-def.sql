CREATE TABLE IF NOT EXISTS events_004 (
	-- App
	`app_id` varchar(255) comment 'lookup', -- 'lookup' is a varchar optimisation for Infobright
	`platform` varchar(50) comment 'lookup',
	-- Date/time
	`dt` date,
	`tm` time,
	-- Event
	`event` varchar(255) comment 'lookup',
	`event_id` varchar(38),
	`txn_id` int,
	-- Versioning
	`v_tracker` varchar(100) comment 'lookup',
	`v_collector` varchar(100) comment 'lookup',
	`v_etl` varchar(100) comment 'lookup',
	-- User and visit
	`user_id` varchar(16), -- Removed 'lookup' in 0.0.4
	`user_ipaddress` varchar(19), -- Removed 'lookup' in 0.0.4
	`user_fingerprint` varchar(50), -- Removed 'lookup' in 0.0.4
	`visit_id` smallint,
	-- Page
	`page_url` varchar(4095), -- Widened, removed 'lookup' in 0.0.4
	`page_title` varchar(2083), -- Removed 'lookup' in 0.0.4
	`page_referrer` varchar(2083), -- Removed 'lookup' in 0.0.4
	-- Marketing
	`mkt_source` varchar(255), -- Removed 'lookup' in 0.0.4
	`mkt_medium` varchar(255), -- Removed 'lookup' in 0.0.4
	`mkt_term` varchar(255), -- Removed 'lookup' in 0.0.4
	`mkt_content` varchar(2083), -- Removed 'lookup' in 0.0.4
	`mkt_campaign` varchar(255), -- Removed 'lookup' in 0.0.4
	-- Custom Event
	`ev_category` varchar(255), -- Removed 'lookup' in 0.0.4
	`ev_action` varchar(255), -- Removed 'lookup' in 0.0.4
	`ev_label` varchar(255), -- Removed 'lookup' in 0.0.4
	`ev_property` varchar(255), -- Removed 'lookup' in 0.0.4
	`ev_value` float,
	-- Ecommerce
	`tr_orderid` varchar(255), -- Removed 'lookup' in 0.0.4
	`tr_affiliation` varchar(255),
	`tr_total` dec(18,2),
	`tr_tax` dec(18,2),
	`tr_shipping` dec(18,2),
	`tr_city` varchar(255), -- Removed 'lookup' in 0.0.4
	`tr_state` varchar(255), -- Removed 'lookup' in 0.0.4
	`tr_country` varchar(255), -- Removed 'lookup' in 0.0.4
	`ti_orderid` varchar(255), -- Removed 'lookup' in 0.0.4
	`ti_sku` varchar(255), -- Removed 'lookup' in 0.0.4
	`ti_name` varchar(255), -- Removed 'lookup' in 0.0.4
	`ti_category` varchar(255), -- Removed 'lookup' in 0.0.4
	`ti_price` dec(18,2),
	`ti_quantity` int,
	-- User Agent
	`useragent` varchar(2083), -- Removed 'lookup' in 0.0.4
	-- Browser
	`br_name` varchar(50) comment 'lookup',
	`br_family` varchar(50) comment 'lookup',
	`br_version` varchar(50) comment 'lookup',
	`br_type` varchar(50) comment 'lookup',
	`br_renderengine` varchar(50) comment 'lookup',
	`br_lang` varchar(255) comment 'lookup', -- Widened in 0.0.4
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
	`br_colordepth` varchar(12) comment 'lookup',
	-- Operating System
	`os_name` varchar(50) comment 'lookup',
	`os_family` varchar(50) comment 'lookup',
	`os_manufacturer` varchar(50) comment 'lookup',
	`os_timezone` varchar(255) comment 'lookup',
	-- Device/Hardware
	`dvce_type` varchar(50) comment 'lookup',
	`dvce_ismobile` tinyint(1),
	`dvce_screenwidth` mediumint,
	`dvce_screenheight` mediumint
) ENGINE=BRIGHTHOUSE DEFAULT CHARSET=utf8 ;
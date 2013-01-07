CREATE TABLE IF NOT EXISTS events_005 (
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
	`user_id` varchar(38), -- Widened in 0.0.5 to support UUID
	`user_ipaddress` varchar(19),
	`user_fingerprint` varchar(50),
	`visit_id` smallint,
	-- Page
	`page_url` varchar(4095),
	`page_title` varchar(2083),
	`page_referrer` varchar(2083),
	-- Marketing
	`mkt_source` varchar(255),
	`mkt_medium` varchar(255),
	`mkt_term` varchar(255),
	`mkt_content` varchar(2083),
	`mkt_campaign` varchar(255),
	-- Custom Event
	`ev_category` varchar(255),
	`ev_action` varchar(255),
	`ev_label` varchar(255),
	`ev_property` varchar(255),
	`ev_value` float,
	-- Ecommerce
	`tr_orderid` varchar(255),
	`tr_affiliation` varchar(255),
	`tr_total` dec(18,2),
	`tr_tax` dec(18,2),
	`tr_shipping` dec(18,2),
	`tr_city` varchar(255),
	`tr_state` varchar(255),
	`tr_country` varchar(255),
	`ti_orderid` varchar(255),
	`ti_sku` varchar(255),
	`ti_name` varchar(255),
	`ti_category` varchar(255),
	`ti_price` dec(18,2),
	`ti_quantity` int,
	-- User Agent
	`useragent` varchar(2083),
	-- Browser
	`br_name` varchar(50) comment 'lookup',
	`br_family` varchar(50) comment 'lookup',
	`br_version` varchar(50) comment 'lookup',
	`br_type` varchar(50) comment 'lookup',
	`br_renderengine` varchar(50) comment 'lookup',
	`br_lang` varchar(255) comment 'lookup',
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
-- Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
-- Version:     Ports version 0.3.0 to version 0.4.0
-- URL:         -
--
-- Authors:     Fred Blundun
-- Copyright:   Copyright (c) 2014 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

-- Rename existing table
ALTER TABLE "atomic"."events" RENAME TO "events_030";

-- Create the table
CREATE TABLE "atomic"."events" (
	-- App
	"app_id" varchar(255),
	"platform" varchar(255),
	-- Date/time
	"etl_tstamp" timestamp,
	"collector_tstamp" timestamp NOT NULL,
	"dvce_tstamp" timestamp,
	-- Date/time
	"event" varchar(128),
	"event_id" char(36) NOT NULL,
	"txn_id" integer,
	-- Versioning
	"name_tracker" varchar(128),
	"v_tracker" varchar(100),
	"v_collector" varchar(100) NOT NULL,
	"v_etl" varchar(100) NOT NULL,
	-- User and visit
	"user_id" varchar(255),
	"user_ipaddress" varchar(45),           -- Increased from 19 in 0.4.0 to support IPv6 addresses
	"user_fingerprint" varchar(50),
	"domain_userid" varchar(36),            -- Increased from 16 in 0.4.0 to support UUIDs
	"domain_sessionidx" smallint,
	"network_userid" varchar(38),
	-- Location
	"geo_country" char(2),
	"geo_region" char(2),
	"geo_city" varchar(75),
	"geo_zipcode" varchar(15),
	"geo_latitude" double precision,
	"geo_longitude" double precision,
	"geo_region_name" varchar(100),
	-- IP lookups
	"ip_isp" varchar(100),
	"ip_organization" varchar(100),
	"ip_domain" varchar(100),
	"ip_netspeed" varchar(100),
	-- Page
	"page_url" text,
	"page_title" varchar(2000),
	"page_referrer" text,
	-- Page URL components
	"page_urlscheme" varchar(16),
	"page_urlhost" varchar(255),
	"page_urlport" integer,
	"page_urlpath" varchar(3000),           -- Increased from 1000 in 0.5.0
	"page_urlquery" varchar(6000),          -- Increased from 3000 in 0.5.0
	"page_urlfragment" varchar(3000),       -- Increased from 255 in 0.5.0
	-- Referrer URL components
	"refr_urlscheme" varchar(16),
	"refr_urlhost" varchar(255),
	"refr_urlport" integer,
	"refr_urlpath" varchar(6000),           -- Increased from 1000 in 0.5.0
	"refr_urlquery" varchar(6000),          -- Increased from 3000 in 0.5.0
	"refr_urlfragment" varchar(3000),       -- Increased from 255 in 0.5.0
	-- Referrer details
	"refr_medium" varchar(25),
	"refr_source" varchar(50),
	"refr_term" varchar(255),
	-- Marketing
	"mkt_medium" varchar(255),
	"mkt_source" varchar(255),
	"mkt_term" varchar(255),
	"mkt_content" varchar(500),
	"mkt_campaign" varchar(255),
	-- Custom contexts
	"contexts" json,
	-- Custom structured event
	"se_category" varchar(1000),            -- Increased from 255 in 0.5.0
	"se_action" varchar(1000),              -- Increased from 3000 in 0.5.0
	"se_label" varchar(1000),               -- Increased from 3000 in 0.5.0
	"se_property" varchar(1000),            -- Increased from 3000 in 0.5.0
	"se_value" double precision,
	-- Custom unstructured event
	"unstruct_event" json,
	-- Ecommerce
	"tr_orderid" varchar(255),
	"tr_affiliation" varchar(255),
	"tr_total" decimal(18,2),
	"tr_tax" decimal(18,2),
	"tr_shipping" decimal(18,2),
	"tr_city" varchar(255),
	"tr_state" varchar(255),
	"tr_country" varchar(255),
	"ti_orderid" varchar(255),
	"ti_sku" varchar(255),
	"ti_name" varchar(255),
	"ti_category" varchar(255),
	"ti_price" decimal(18,2),
	"ti_quantity" integer,
	-- Page ping
	"pp_xoffset_min" integer,
	"pp_xoffset_max" integer,
	"pp_yoffset_min" integer,
	"pp_yoffset_max" integer,
	-- User Agent
	"useragent" varchar(1000),
	-- Browser
	"br_name" varchar(50),
	"br_family" varchar(50),
	"br_version" varchar(50),
	"br_type" varchar(50),
	"br_renderengine" varchar(50),
	"br_lang" varchar(255),
	"br_features_pdf" boolean,
	"br_features_flash" boolean,
	"br_features_java" boolean,
	"br_features_director" boolean,
	"br_features_quicktime" boolean,
	"br_features_realplayer" boolean,
	"br_features_windowsmedia" boolean,
	"br_features_gears" boolean,
	"br_features_silverlight" boolean,
	"br_cookies" boolean,
	"br_colordepth" varchar(12),
	"br_viewwidth" integer,
	"br_viewheight" integer,
	-- Operating System
	"os_name" varchar(50),
	"os_family" varchar(50),
	"os_manufacturer" varchar(50),
	"os_timezone" varchar(50),
	-- Device/Hardware
	"dvce_type" varchar(50),
	"dvce_ismobile" boolean,
	"dvce_screenwidth" integer,
	"dvce_screenheight" integer,
	-- Document
	"doc_charset" varchar(128),
	"doc_width" integer,
	"doc_height" integer,
	-- Currency
	"tr_currency" char(3),                  -- Added in 0.4.0
	"tr_total_base" decimal(18, 2),         -- Added in 0.4.0
	"tr_tax_base" decimal(18, 2),           -- Added in 0.4.0
	"tr_shipping_base" decimal(18, 2),      -- Added in 0.4.0
	"ti_currency" char(3),                  -- Added in 0.4.0
	"ti_price_base" decimal(18, 2),         -- Added in 0.4.0
	"base_currency" char(3),                -- Added in 0.4.0
	-- Geolocation
	"geo_timezone" varchar(64),             -- Added in 0.4.0
	-- Click ID
	"mkt_clickid" varchar(64),              -- Added in 0.4.0
	"mkt_network" varchar(64),              -- Added in 0.4.0
	-- ETL tags
	"etl_tags" varchar(500),                -- Added in 0.4.0
	-- Time event was sent
	"dvce_sent_tstamp" timestamp,           -- Added in 0.4.0
	-- Referer
	"refr_domain_userid" varchar(36),       -- Added in 0.4.0
	"refr_dvce_tstamp" timestamp,           -- Added in 0.4.0
	-- Derived contexts
	"derived_contexts" json,                -- Added in 0.4.0
	-- Session ID
	"domain_sessionid" char(36),            -- Added in 0.4.0
	-- Derived timestamp
	"derived_tstamp" timestamp              -- Added in 0.4.0
	                                        -- Removed primary key constraint on event_id in 0.4.0
)
WITH (OIDS=FALSE)
;

-- Now copy into new from events_old
INSERT INTO atomic.events
	SELECT
	"app_id",
	"platform",
	"etl_tstamp",
	"collector_tstamp",
	"dvce_tstamp",
	"event",
	"event_id",
	"txn_id",
	"name_tracker",
	"v_tracker",
	"v_collector",
	"v_etl",
	"user_id",
	"user_ipaddress",
	"user_fingerprint",
	"domain_userid",
	"domain_sessionidx",
	"network_userid",
	"geo_country",
	"geo_region",
	"geo_city",
	"geo_zipcode",
	"geo_latitude",
	"geo_longitude",
	"geo_region_name",
	"ip_isp",
	"ip_organization",
	"ip_domain",
	"ip_netspeed",	
	"page_url",
	"page_title",
	"page_referrer",
	"page_urlscheme",
	"page_urlhost",
	"page_urlport",
	"page_urlpath",
	"page_urlquery",
	"page_urlfragment",
	"refr_urlscheme",
	"refr_urlhost",
	"refr_urlport",
	"refr_urlpath",
	"refr_urlquery",
	"refr_urlfragment",
	"refr_medium",
	"refr_source",
	"refr_term",
	"mkt_medium",
	"mkt_source",
	"mkt_term",
	"mkt_content",
	"mkt_campaign",
	"contexts",
	"se_category",
	"se_action",
	"se_label",
	"se_property",
	"se_value",
	"unstruct_event",
	"tr_orderid",
	"tr_affiliation",
	"tr_total",
	"tr_tax",
	"tr_shipping",
	"tr_city",
	"tr_state",
	"tr_country",
	"ti_orderid",
	"ti_sku",
	"ti_name",
	"ti_category",
	"ti_price",
	"ti_quantity",
	"pp_xoffset_min",
	"pp_xoffset_max",
	"pp_yoffset_min",
	"pp_yoffset_max",
	"useragent",
	"br_name",
	"br_family",
	"br_version",
	"br_type",
	"br_renderengine",
	"br_lang",
	"br_features_pdf",
	"br_features_flash",
	"br_features_java",
	"br_features_director",
	"br_features_quicktime",
	"br_features_realplayer",
	"br_features_windowsmedia",
	"br_features_gears",
	"br_features_silverlight",
	"br_cookies",
	"br_colordepth",
	"br_viewwidth",
	"br_viewheight",
	"os_name",
	"os_family",
	"os_manufacturer",
	"os_timezone",
	"dvce_type",
	"dvce_ismobile",
	"dvce_screenwidth",
	"dvce_screenheight",
	"doc_charset",
	"doc_width",
	"doc_height",
	NULL AS "tr_currency",                  -- Added in 0.4.0
	NULL AS "tr_total_base",                -- Added in 0.4.0
	NULL AS "tr_tax_base",                  -- Added in 0.4.0
	NULL AS "tr_shipping_base",             -- Added in 0.4.0
	NULL AS "ti_currency",                  -- Added in 0.4.0
	NULL AS "ti_price_base",                -- Added in 0.4.0
	NULL AS "base_currency",                -- Added in 0.4.0
	NULL AS "geo_timezone",                 -- Added in 0.4.0
	NULL AS "mkt_clickid",                  -- Added in 0.4.0
	NULL AS "mkt_network",                  -- Added in 0.4.0
	NULL AS "etl_tags",                     -- Added in 0.4.0
	NULL AS "dvce_sent_tstamp",             -- Added in 0.4.0
	NULL AS "refr_domain_userid",           -- Added in 0.4.0
	NULL AS "refr_dvce_tstamp",             -- Added in 0.4.0
	NULL AS "derived_contexts",             -- Added in 0.4.0
	NULL AS "domain_sessionid",             -- Added in 0.4.0
	NULL AS "derived_tstamp"                -- Added in 0.4.0

    FROM atomic.events_030;

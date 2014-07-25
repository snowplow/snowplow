-- Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
-- Version:     Ports version 0.2.0 to version 0.3.0
-- URL:         -
--
-- Authors:     Fred Blundun
-- Copyright:   Copyright (c) 2014 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

-- Rename existing table
ALTER TABLE "atomic"."events" RENAME TO "events_020";

-- Create the table
CREATE TABLE "atomic"."events" (
	-- App
	"app_id" varchar(255),
	"platform" varchar(255),
	-- Date/time
	"etl_tstamp" timestamp,                 -- Added in 0.3.0
	"collector_tstamp" timestamp NOT NULL,
	"dvce_tstamp" timestamp,
	-- Date/time
	"event" varchar(128),
	                                        -- Removed event_vendor in 0.3.0
	"event_id" char(36) NOT NULL,
	"txn_id" integer,
	-- Versioning
	"name_tracker" varchar(128),
	"v_tracker" varchar(100),
	"v_collector" varchar(100) NOT NULL,
	"v_etl" varchar(100) NOT NULL,
	-- User and visit
	"user_id" varchar(255),
	"user_ipaddress" varchar(19),
	"user_fingerprint" varchar(50),
	"domain_userid" varchar(16),
	"domain_sessionidx" smallint,
	"network_userid" varchar(38),
	-- Location
	"geo_country" char(2),
	"geo_region" char(2),
	"geo_city" varchar(75),
	"geo_zipcode" varchar(15),
	"geo_latitude" double precision,
	"geo_longitude" double precision,
	"geo_region_name" varchar(100),         -- Added in 0.3.0
	-- IP lookups
	"ip_isp" varchar(100),                  -- Added in 0.3.0
	"ip_organization" varchar(100),         -- Added in 0.3.0
	"ip_domain" varchar(100),               -- Added in 0.3.0
	"ip_netspeed" varchar(100),             -- Added in 0.3.0
	-- Page
	"page_url" text,
	"page_title" varchar(2000),
	"page_referrer" text,
	-- Page URL components
	"page_urlscheme" varchar(16),
	"page_urlhost" varchar(255),
	"page_urlport" integer,
	"page_urlpath" varchar(1000),
	"page_urlquery" varchar(3000),
	"page_urlfragment" varchar(255),
	-- Referrer URL components
	"refr_urlscheme" varchar(16),
	"refr_urlhost" varchar(255),
	"refr_urlport" integer,
	"refr_urlpath" varchar(1000),
	"refr_urlquery" varchar(3000),
	"refr_urlfragment" varchar(255),
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
	"se_category" varchar(255),
	"se_action" varchar(255),
	"se_label" varchar(255),
	"se_property" varchar(255),
	"se_value" double precision,
	-- Custom unstructured event
	                                        -- Removed ue_name in 0.3.0
	"unstruct_event" json,                  -- Renamed ue_properties to unstruct_event in 0.3.0
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
	PRIMARY KEY ("event_id")
)
WITH (OIDS=FALSE)
;

-- Now copy into new from events_old
INSERT INTO atomic.events
	SELECT
	"app_id",
	"platform",
	NULL AS "etl_tstamp",
	"collector_tstamp",
	"dvce_tstamp",
	"event",
	                                        -- Removed event_vendor in 0.3.0
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
	NULL AS "geo_region_name",              -- Added in 0.3.0
	NULL AS "ip_isp",                       -- Added in 0.3.0
	NULL AS "ip_organization",              -- Added in 0.3.0
	NULL AS "ip_domain",                    -- Added in 0.3.0	
	NULL AS "ip_netspeed",                  -- Added in 0.3.0	
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
	                                        -- Removed ue_name in 0.3.0
	"ue_properties" AS "unstruct_event",    -- Renamed ue_properties to unstruct_event in 0.3.0
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
	"doc_height"
    FROM atomic.events_020;

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
-- Version:     0.2.0
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean
-- Copyright:   Copyright (c) 2013 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

CREATE SCHEMA "atomic";

CREATE TABLE "atomic"."events" (
	-- App
	"app_id" varchar(255),
	"platform" varchar(255),
	-- Date/time
	"collector_tstamp" timestamp NOT NULL,
	"dvce_tstamp" timestamp,
	-- Date/time
	"event" varchar(128),
	"event_vendor" varchar(128),            -- Removed not null constraint in 0.3.0
	"event_id" char(36) NOT NULL,           -- Changed from varchar(38) in 0.2.0
	"txn_id" integer,
	-- Versioning
	"name_tracker" varchar(128),            -- Added in 0.2.0
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
	-- Page
	"page_url" text,                        -- Added in 0.2.0
	"page_title" varchar(2000),
	"page_referrer" text,                   -- Added in 0.2.0
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
	"contexts" json,                        -- Added in 0.2.0. Consider jsonb type coming soon to PG
	-- Custom structured event
	"se_category" varchar(255),
	"se_action" varchar(255),
	"se_label" varchar(255),
	"se_property" varchar(255),
	"se_value" double precision,
	-- Custom unstructured event
	"ue_name" varchar(255),                 -- Added in 0.2.0
	"ue_properties" json,                   -- Added in 0.2.0. Consider jsonb type coming soon to PG
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

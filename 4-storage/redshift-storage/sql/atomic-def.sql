-- Copyright (c) 2013-2018 Snowplow Analytics Ltd. All rights reserved.
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
-- Version:     0.10.0
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean, Peter van Wesep, Fred Blundun, Konstantinos Servis
-- Copyright:   Copyright (c) 2013-2018 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

-- Create the schema
CREATE SCHEMA atomic;

-- Create events table
CREATE TABLE atomic.events (
	-- App
	app_id varchar(255) encode ZSTD,
	platform varchar(255) encode ZSTD,
	-- Date/time
	etl_tstamp timestamp  encode ZSTD,
	collector_tstamp timestamp not null encode RAW,
	dvce_created_tstamp timestamp encode ZSTD,
	-- Event
	event varchar(128) encode ZSTD,
	event_id char(36) not null unique encode ZSTD,
	txn_id int encode ZSTD,
	-- Namespacing and versioning
	name_tracker varchar(128) encode ZSTD,
	v_tracker varchar(100) encode ZSTD,
	v_collector varchar(100) encode ZSTD not null,
	v_etl varchar(100) encode ZSTD not null,
	-- User and visit
	user_id varchar(255) encode ZSTD,
	user_ipaddress varchar(128) encode ZSTD,
	user_fingerprint varchar(128) encode ZSTD,
	domain_userid varchar(128) encode ZSTD,
	domain_sessionidx int encode ZSTD,
	network_userid varchar(128) encode ZSTD,
	-- Location
	geo_country char(2) encode ZSTD,
	geo_region char(2) encode ZSTD,
	geo_city varchar(75) encode ZSTD,
	geo_zipcode varchar(15) encode ZSTD,
	geo_latitude double precision encode ZSTD,
	geo_longitude double precision encode ZSTD,
	geo_region_name varchar(100) encode ZSTD,
	-- IP lookups
	ip_isp varchar(100) encode ZSTD,
	ip_organization varchar(128) encode ZSTD,
	ip_domain varchar(128) encode ZSTD,
	ip_netspeed varchar(100) encode ZSTD,
	-- Page
	page_url varchar(4096) encode ZSTD,
	page_title varchar(2000) encode ZSTD,
	page_referrer varchar(4096) encode ZSTD,
	-- Page URL components
	page_urlscheme varchar(16) encode ZSTD,
	page_urlhost varchar(255) encode ZSTD,
	page_urlport int encode ZSTD,
	page_urlpath varchar(3000) encode ZSTD,
	page_urlquery varchar(6000) encode ZSTD,
	page_urlfragment varchar(3000) encode ZSTD,
	-- Referrer URL components
	refr_urlscheme varchar(16) encode ZSTD,
	refr_urlhost varchar(255) encode ZSTD,
	refr_urlport int encode ZSTD,
	refr_urlpath varchar(6000) encode ZSTD,
	refr_urlquery varchar(6000) encode ZSTD,
	refr_urlfragment varchar(3000) encode ZSTD,
	-- Referrer details
	refr_medium varchar(25) encode ZSTD,
	refr_source varchar(50) encode ZSTD,
	refr_term varchar(255) encode ZSTD,
	-- Marketing
	mkt_medium varchar(255) encode ZSTD,
	mkt_source varchar(255) encode ZSTD,
	mkt_term varchar(255) encode ZSTD,
	mkt_content varchar(500) encode ZSTD,
	mkt_campaign varchar(255) encode ZSTD,
	-- Custom structured event
	se_category varchar(1000) encode ZSTD,
	se_action varchar(1000) encode ZSTD,
	se_label varchar(4096) encode ZSTD,
	se_property varchar(1000) encode ZSTD,
	se_value double precision encode ZSTD,
	-- Ecommerce
	tr_orderid varchar(255) encode ZSTD,
	tr_affiliation varchar(255) encode ZSTD,
	tr_total dec(18,2) encode ZSTD,
	tr_tax dec(18,2) encode ZSTD,
	tr_shipping dec(18,2) encode ZSTD,
	tr_city varchar(255) encode ZSTD,
	tr_state varchar(255) encode ZSTD,
	tr_country varchar(255) encode ZSTD,
	ti_orderid varchar(255) encode ZSTD,
	ti_sku varchar(255) encode ZSTD,
	ti_name varchar(255) encode ZSTD,
	ti_category varchar(255) encode ZSTD,
	ti_price dec(18,2) encode ZSTD,
	ti_quantity int encode ZSTD,
	-- Page ping
	pp_xoffset_min int encode ZSTD,
	pp_xoffset_max int encode ZSTD,
	pp_yoffset_min int encode ZSTD,
	pp_yoffset_max int encode ZSTD,
	-- User Agent
	useragent varchar(1000) encode ZSTD,
	-- Browser
	br_name varchar(50) encode ZSTD,
	br_family varchar(50) encode ZSTD,
	br_version varchar(50) encode ZSTD,
	br_type varchar(50) encode ZSTD,
	br_renderengine varchar(50) encode ZSTD,
	br_lang varchar(255) encode ZSTD,
	br_features_pdf boolean encode ZSTD,
	br_features_flash boolean encode ZSTD,
	br_features_java boolean encode ZSTD,
	br_features_director boolean encode ZSTD,
	br_features_quicktime boolean encode ZSTD,
	br_features_realplayer boolean encode ZSTD,
	br_features_windowsmedia boolean encode ZSTD,
	br_features_gears boolean encode ZSTD,
	br_features_silverlight boolean encode ZSTD,
	br_cookies boolean encode ZSTD,
	br_colordepth varchar(12) encode ZSTD,
	br_viewwidth int encode ZSTD,
	br_viewheight int encode ZSTD,
	-- Operating System
	os_name varchar(50) encode ZSTD,
	os_family varchar(50)  encode ZSTD,
	os_manufacturer varchar(50)  encode ZSTD,
	os_timezone varchar(255)  encode ZSTD,
	-- Device/Hardware
	dvce_type varchar(50)  encode ZSTD,
	dvce_ismobile boolean encode ZSTD,
	dvce_screenwidth int encode ZSTD,
	dvce_screenheight int encode ZSTD,
	-- Document
	doc_charset varchar(128) encode ZSTD,
	doc_width int encode ZSTD,
	doc_height int encode ZSTD,

	-- Currency
	tr_currency char(3) encode ZSTD,
	tr_total_base dec(18, 2) encode ZSTD,
	tr_tax_base dec(18, 2) encode ZSTD,
	tr_shipping_base dec(18, 2) encode ZSTD,
	ti_currency char(3) encode ZSTD,
	ti_price_base dec(18, 2) encode ZSTD,
	base_currency char(3) encode ZSTD,

	-- Geolocation
	geo_timezone varchar(64) encode ZSTD,

	-- Click ID
	mkt_clickid varchar(128) encode ZSTD,
	mkt_network varchar(64) encode ZSTD,

	-- ETL tags
	etl_tags varchar(500) encode ZSTD,

	-- Time event was sent
	dvce_sent_tstamp timestamp encode ZSTD,

	-- Referer
	refr_domain_userid varchar(128) encode ZSTD,
	refr_dvce_tstamp timestamp encode ZSTD,

	-- Session ID
	domain_sessionid char(128) encode ZSTD,

	-- Derived timestamp
	derived_tstamp timestamp encode ZSTD,

	-- Event schema
	event_vendor varchar(1000) encode ZSTD,
	event_name varchar(1000) encode ZSTD,
	event_format varchar(128) encode ZSTD,
	event_version varchar(128) encode ZSTD,

	-- Event fingerprint
	event_fingerprint varchar(128) encode ZSTD,

	-- True timestamp
	true_tstamp timestamp encode ZSTD,

	CONSTRAINT event_id_0100_pk PRIMARY KEY(event_id)
)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (collector_tstamp);

COMMENT ON TABLE "atomic"."events" IS '0.10.0';

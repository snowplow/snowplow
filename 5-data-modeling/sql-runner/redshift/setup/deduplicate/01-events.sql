-- Copyright (c) 2013-2015 Snowplow Analytics Ltd. All rights reserved.
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
-- Authors: Christophe Bogaert
-- Copyright: Copyright (c) 2015 Snowplow Analytics Ltd
-- License: Apache License Version 2.0
--
-- Data Model: deduplicate
-- Version: 0.2
--
-- Duplicated events:
-- (a) create the duplicated events table (identical to atomic.events 0.6)

CREATE TABLE IF NOT EXISTS duplicates.events (

	-- App
	app_id varchar(255) encode lzo,
	platform varchar(255) encode lzo,

	-- Date/time
	etl_tstamp timestamp encode lzo,
	collector_tstamp timestamp not null encode lzo,
	dvce_tstamp timestamp encode lzo,

	-- Event
	event varchar(128) encode lzo,
	event_id char(36) not null unique encode raw,
	txn_id int encode lzo,

	-- Namespacing and versioning
	name_tracker varchar(128) encode lzo,
	v_tracker varchar(100) encode lzo,
	v_collector varchar(100) not null encode runlength,
	v_etl varchar(100) not null encode runlength,

	-- User and visit
	user_id varchar(255) encode lzo,
	user_ipaddress varchar(45) encode lzo,
	user_fingerprint varchar(50) encode lzo,
	domain_userid varchar(36) encode lzo,
	domain_sessionidx smallint encode lzo,
	network_userid varchar(38) encode lzo,

	-- Location
	geo_country char(2) encode lzo,
	geo_region char(2) encode lzo,
	geo_city varchar(75) encode lzo,
	geo_zipcode varchar(15) encode lzo,
	geo_latitude double precision encode bytedict, -- alternative: encode raw
	geo_longitude double precision encode bytedict, -- alternative: encode raw
	geo_region_name varchar(100) encode lzo,

	-- IP lookups
	ip_isp varchar(100) encode lzo,
	ip_organization varchar(100) encode lzo,
	ip_domain varchar(100) encode lzo,
	ip_netspeed varchar(100) encode lzo,

	-- Page
	page_url varchar(4096) encode lzo,
	page_title varchar(2000) encode lzo,
	page_referrer varchar(4096) encode lzo,

	-- Page URL components
	page_urlscheme varchar(16) encode lzo,
	page_urlhost varchar(255) encode lzo,
	page_urlport int encode lzo,
	page_urlpath varchar(3000) encode lzo,
	page_urlquery varchar(6000) encode lzo,
	page_urlfragment varchar(3000) encode lzo,

	-- Referrer URL components
	refr_urlscheme varchar(16) encode lzo,
	refr_urlhost varchar(255) encode lzo,
	refr_urlport int encode lzo,
	refr_urlpath varchar(6000) encode lzo,
	refr_urlquery varchar(6000) encode lzo,
	refr_urlfragment varchar(3000) encode lzo,

	-- Referrer details
	refr_medium varchar(25) encode lzo,
	refr_source varchar(50) encode lzo,
	refr_term varchar(255) encode lzo,

	-- Marketing
	mkt_medium varchar(255) encode lzo,
	mkt_source varchar(255) encode lzo,
	mkt_term varchar(255) encode lzo,
	mkt_content varchar(500) encode lzo,
	mkt_campaign varchar(255) encode lzo,

	-- Custom contexts

	contexts varchar(15000) encode lzo,

	-- Custom structured event
	se_category varchar(1000) encode lzo,
	se_action varchar(1000) encode lzo,
	se_label varchar(1000) encode lzo,
	se_property varchar(1000) encode lzo,
	se_value double precision encode bytedict, -- alternative: encode raw

	-- Custom unstructured event
	unstruct_event varchar(15000) encode lzo,

	-- Ecommerce
	tr_orderid varchar(255) encode lzo,
	tr_affiliation varchar(255) encode lzo,
	tr_total dec(18,2) encode lzo,
	tr_tax dec(18,2) encode lzo,
	tr_shipping dec(18,2) encode lzo,
	tr_city varchar(255) encode lzo,
	tr_state varchar(255) encode lzo,
	tr_country varchar(255) encode lzo,
	ti_orderid varchar(255) encode lzo,
	ti_sku varchar(255) encode lzo,
	ti_name varchar(255) encode lzo,
	ti_category varchar(255) encode lzo,
	ti_price dec(18,2) encode lzo,
	ti_quantity int encode lzo,

	-- Page ping
	pp_xoffset_min integer encode lzo,
	pp_xoffset_max integer encode lzo,
	pp_yoffset_min integer encode lzo,
	pp_yoffset_max integer encode lzo,

	-- User Agent
	useragent varchar(1000) encode lzo,

	-- Browser
	br_name varchar(50) encode lzo,
	br_family varchar(50) encode lzo,
	br_version varchar(50) encode lzo,
	br_type varchar(50) encode lzo,
	br_renderengine varchar(50) encode lzo,
	br_lang varchar(255) encode lzo,
	br_features_pdf boolean encode raw,
	br_features_flash boolean encode raw,
	br_features_java boolean encode raw,
	br_features_director boolean encode raw,
	br_features_quicktime boolean encode raw,
	br_features_realplayer boolean encode raw,
	br_features_windowsmedia boolean encode raw,
	br_features_gears boolean encode raw,
	br_features_silverlight boolean encode raw,
	br_cookies boolean encode runlength, -- alternative: encode raw
	br_colordepth varchar(12) encode bytedict, -- alternative: encode lzo
	br_viewwidth integer encode bytedict, -- alternative: encode lzo
	br_viewheight integer encode lzo,

	-- Operating System
	os_name varchar(50) encode lzo,
	os_family varchar(50) encode lzo,
	os_manufacturer varchar(50) encode lzo,
	os_timezone varchar(255) encode lzo,

	-- Device/Hardware
	dvce_type varchar(50) encode lzo,
	dvce_ismobile boolean encode raw,
	dvce_screenwidth integer encode bytedict, -- alternative: encode lzo
	dvce_screenheight integer encode bytedict, -- alternative: encode lzo

	-- Document
	doc_charset varchar(128) encode lzo,
	doc_width integer encode bytedict, -- alternative: encode lzo
	doc_height integer encode lzo, -- alternative: encode delta32k

	-- Currency
	tr_currency char(3) encode bytedict,
	tr_total_base dec(18, 2) encode lzo,
	tr_tax_base dec(18, 2) encode lzo,
	tr_shipping_base dec(18, 2) encode lzo,
	ti_currency char(3) encode bytedict,
	ti_price_base dec(18, 2) encode lzo,
	base_currency char(3) encode bytedict,

	-- Geolocation
	geo_timezone varchar(64) encode lzo,

	-- Click ID
	mkt_clickid varchar(128) encode lzo,
	mkt_network varchar(64) encode lzo,

	-- ETL tags
	etl_tags varchar(500) encode lzo,

	-- Time event was sent
	dvce_sent_tstamp timestamp encode lzo,

	-- Referer
	refr_domain_userid varchar(36) encode lzo,
	refr_dvce_tstamp timestamp encode lzo,

	-- Derived contexts
	derived_contexts varchar(15000) encode lzo,

	-- Session ID
	domain_sessionid char(36) encode lzo,

	-- Derived timestamp
	derived_tstamp timestamp encode lzo
)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (collector_tstamp);

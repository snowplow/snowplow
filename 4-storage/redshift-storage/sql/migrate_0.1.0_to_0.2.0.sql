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
-- Version:     Ports version 0.1.0 to version 0.2.0
-- URL:         -
--
-- Authors:     Alex Dean
-- Copyright:   Copyright (c) 2013 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

-- First rename the existing table (don't delete it)
ALTER TABLE events DROP CONSTRAINT event_id_pk;
ALTER TABLE events RENAME TO events_010;

-- Now create the new table (copy-and-pasted from table-def.sql)
CREATE TABLE events (
	-- App
	app_id varchar(255) encode text255, 
	platform varchar(255) encode text255, 
	-- Date/time
	collector_tstamp timestamp not null,
	dvce_tstamp timestamp,
	-- Event
	event varchar(128) encode text255,
	event_vendor varchar(128) encode text32k not null,
	event_id varchar(38) not null unique,
	txn_id int,
	-- Versioning
	v_tracker varchar(100) encode text255, 
	v_collector varchar(100) encode text255 not null,
	v_etl varchar(100) encode text255 not null, 
	-- User and visit
	user_id varchar(255) encode runlength, 
	user_ipaddress varchar(19) encode runlength,
	user_fingerprint varchar(50) encode runlength,
	domain_userid varchar(16),
	domain_sessionidx smallint,
	network_userid varchar(38),
	-- Location
	geo_country char(2),            -- New in 0.2.0
	geo_region char(2),             -- New in 0.2.0
	geo_city varchar(75),           -- New in 0.2.0
	geo_zipcode varchar(15),        -- New in 0.2.0
	geo_latitude double precision,  -- New in 0.2.0
	geo_longitude double precision, -- New in 0.2.0
	-- Page
	page_title varchar(2000),
	-- Page URL components
	page_urlscheme varchar(16) encode text255,    
	page_urlhost varchar(255) encode text255,     
	page_urlport smallint,        
	page_urlpath varchar(1000) encode text32k,
	page_urlquery varchar(3000),
	page_urlfragment varchar(255),
	-- Referrer URL components
	refr_urlscheme varchar(16) encode text255,    
	refr_urlhost varchar(255) encode text255,     
	refr_urlport smallint,        
	refr_urlpath varchar(1000) encode text32k,
	refr_urlquery varchar(3000),
	refr_urlfragment varchar(255),
	-- Referrer details
	refr_medium varchar(25) encode text255,
	refr_source varchar(50) encode text255,
	refr_term varchar(255) encode raw,
	-- Marketing
	mkt_medium varchar(255) encode text255,
	mkt_source varchar(255) encode text255,
	mkt_term varchar(255) encode raw,
	mkt_content varchar(500) encode raw,
	mkt_campaign varchar(255) encode text32k,
	-- Custom structured event
	se_category varchar(255) encode text255, -- Renamed from ev_
	se_action varchar(255) encode text255,   -- Renamed from ev_
	se_label varchar(255) encode text32k,    -- Renamed from ev_
	se_property varchar(255) encode text32k, -- Renamed from ev_
	se_value float,                          -- Renamed from ev_
	-- Ecommerce
	tr_orderid varchar(255) encode raw,
	tr_affiliation varchar(255) encode text255,
	tr_total dec(18,2),
	tr_tax dec(18,2),
	tr_shipping dec(18,2),
	tr_city varchar(255) encode text32k,
	tr_state varchar(255) encode text32k,
	tr_country varchar(255) encode text32k,
	ti_orderid varchar(255) encode raw,
	ti_sku varchar(255) encode text32k,
	ti_name varchar(255) encode text32k,
	ti_category varchar(255) encode text255,
	ti_price dec(18,2),
	ti_quantity int,
	-- Page ping
	pp_xoffset_min integer,
	pp_xoffset_max integer,
	pp_yoffset_min integer,
	pp_yoffset_max integer,
	-- User Agent
	useragent varchar(1000) encode text32k,
	-- Browser
	br_name varchar(50) encode text255,
	br_family varchar(50) encode text255,
	br_version varchar(50) encode text255,
	br_type varchar(50) encode text255,
	br_renderengine varchar(50) encode text255,
	br_lang varchar(255) encode text255,
	br_features_pdf boolean,
	br_features_flash boolean,
	br_features_java boolean,
	br_features_director boolean,
	br_features_quicktime boolean,
	br_features_realplayer boolean,
	br_features_windowsmedia boolean,
	br_features_gears boolean ,
	br_features_silverlight boolean,
	br_cookies boolean,
	br_colordepth varchar(12) encode text255,
	br_viewwidth integer, 
	br_viewheight integer,
	-- Operating System
	os_name varchar(50) encode text255,
	os_family varchar(50)  encode text255,
	os_manufacturer varchar(50)  encode text255,
	os_timezone varchar(255)  encode text255,
	-- Device/Hardware
	dvce_type varchar(50)  encode text255,
	dvce_ismobile boolean,
	dvce_screenwidth integer,
	dvce_screenheight integer,
	-- Document
	doc_charset varchar(128) encode text255,
	doc_width integer,
	doc_height integer,
	CONSTRAINT event_id_pk PRIMARY KEY(event_id)
)
DISTSTYLE KEY
DISTKEY (domain_userid)
SORTKEY (collector_tstamp);

-- Finally copy all the old data into the new format
INSERT INTO events
	SELECT
	-- App
	app_id, 
	platform, 
	-- Date/time
	collector_tstamp,
	dvce_tstamp,
	-- Event
	event,
	event_vendor,
	event_id,
	txn_id,
	-- Versioning
	v_tracker, 
	v_collector,
	v_etl, 
	-- User and visit
	user_id, 
	user_ipaddress,
	user_fingerprint,
	domain_userid,
	domain_sessionidx,
	network_userid,
	-- Location
	null AS geo_country,   -- Placeholder
	null AS geo_region,    -- Placeholder
	null AS geo_city,      -- Placeholder
	null AS geo_zipcode,   -- Placeholder
	null AS geo_latitude,  -- Placeholder
	null AS geo_longitude, -- Placeholder
	-- Page
	page_title,
	-- Page URL components
	page_urlscheme,    
	page_urlhost,     
	page_urlport,        
	page_urlpath,
	page_urlquery,
	page_urlfragment,
	-- Referrer URL components
	refr_urlscheme, 
	refr_urlhost,
	refr_urlport, 
	refr_urlpath, 
	refr_urlquery,
	refr_urlfragment, 
	-- Referrer details
	refr_medium,
	refr_source,
	refr_term,
	-- Marketing
	mkt_medium,
	mkt_source,
	mkt_term,
	mkt_content,
	mkt_campaign,
	-- Custom Event
	ev_category AS se_category, -- Renamed
	ev_action   AS se_action,   -- Renamed
	ev_label    AS se_label,    -- Renamed
	ev_property AS se_property, -- Renamed
	ev_value    AS se_value,    -- Renamed
	-- Ecommerce
	tr_orderid,
	tr_affiliation,
	tr_total,
	tr_tax,
	tr_shipping,
	tr_city,
	tr_state,
	tr_country,
	ti_orderid,
	ti_sku,
	ti_name,
	ti_category,
	ti_price,
	ti_quantity,
	-- Page ping
	pp_xoffset_min,
	pp_xoffset_max,
	pp_yoffset_min,
	pp_yoffset_max,
	-- User Agent
	useragent,
	-- Browser
	br_name,
	br_family,
	br_version,
	br_type,
	br_renderengine,
	br_lang,
	br_features_pdf,
	br_features_flash,
	br_features_java,
	br_features_director,
	br_features_quicktime,
	br_features_realplayer,
	br_features_windowsmedia,
	br_features_gears,
	br_features_silverlight,
	br_cookies,
	br_colordepth,
	br_viewwidth, 
	br_viewheight,
	-- Operating System
	os_name,
	os_family,
	os_manufacturer,
	os_timezone,
	-- Device/Hardware
	dvce_type,
	dvce_ismobile,
	dvce_screenwidth,
	dvce_screenheight,
	-- Document
	doc_charset,
	doc_width,
	doc_height
FROM events_010;
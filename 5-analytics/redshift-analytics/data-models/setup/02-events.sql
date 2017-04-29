
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
-- Version:     0.6.0
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean, Peter van Wesep, Fred Blundun
-- Copyright:   Copyright (c) 2013-2015 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

-- Create the snowplow_landing.events table:
CREATE TABLE snowplow_landing.events (
  -- App
  app_id varchar(255) encode text255,
  platform varchar(255) encode text255,
  -- Date/time
  etl_tstamp timestamp,
  collector_tstamp timestamp not null,
  dvce_tstamp timestamp,
  -- Event
  event varchar(128) encode text255,
  event_id char(36) not null unique,
  txn_id int,
  -- Namespacing and versioning
  name_tracker varchar(128) encode text255,
  v_tracker varchar(100) encode text255,
  v_collector varchar(100) encode text255 not null,
  v_etl varchar(100) encode text255 not null,
  -- User and visit
  user_id varchar(255) encode runlength,
  user_ipaddress varchar(45) encode runlength,       -- Increased from 19 in 0.5.0 to support IPv6 addresses
  user_fingerprint varchar(50) encode runlength,
  domain_userid varchar(36) encode runlength,        -- Increased from 16 in 0.5.0 to support UUIDs
  domain_sessionidx smallint,
  network_userid varchar(38),
  -- Location
  geo_country char(2) encode runlength,
  geo_region char(2) encode runlength,
  geo_city varchar(75) encode runlength,
  geo_zipcode varchar(15) encode runlength,
  geo_latitude double precision encode runlength,
  geo_longitude double precision encode runlength,
  geo_region_name varchar(100) encode runlength,
  -- IP lookups
  ip_isp varchar(100) encode runlength,
  ip_organization varchar(100) encode runlength,
  ip_domain varchar(100) encode runlength,
  ip_netspeed varchar(100) encode runlength,
  -- Page
  page_url varchar(4096),
  page_title varchar(2000),
  page_referrer varchar(4096),
  -- Page URL components
  page_urlscheme varchar(16) encode text255,
  page_urlhost varchar(255) encode text255,
  page_urlport int,
  page_urlpath varchar(3000) encode text32k,         -- Increased from 1000 in 0.5.0
  page_urlquery varchar(6000),                       -- Increased from 3000 in 0.5.0
  page_urlfragment varchar(3000),                    -- Increased from 255 in 0.5.0
  -- Referrer URL components
  refr_urlscheme varchar(16) encode text255,
  refr_urlhost varchar(255) encode text255,
  refr_urlport int,
  refr_urlpath varchar(6000) encode text32k,         -- Increased from 3000 in 0.5.0
  refr_urlquery varchar(6000),                       -- Increased from 3000 in 0.5.0
  refr_urlfragment varchar(3000),                    -- Increased from 255 in 0.5.0
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
  -- Custom contexts
  contexts varchar(15000) encode lzo,                -- Changed encoding from raw to lzo in 0.5.0 and increased size
  -- Custom structured event
  se_category varchar(1000) encode text32k,          -- Increased from 255 in 0.5.0
  se_action varchar(1000) encode text32k,            -- Increased from 255 in 0.5.0
  se_label varchar(1000) encode text32k,             -- Increased from 255 in 0.5.0
  se_property varchar(1000) encode text32k,          -- Increased from 255 in 0.5.0
  se_value double precision,
  -- Custom unstructured event
  unstruct_event varchar(15000) encode lzo,          -- Changed encoding from raw to lzo in 0.5.0
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

  -- Currency
  tr_currency char(3) encode bytedict,               -- Added in 0.5.0
  tr_total_base dec(18, 2),                          -- Added in 0.5.0
  tr_tax_base dec(18, 2),                            -- Added in 0.5.0
  tr_shipping_base dec(18, 2),                       -- Added in 0.5.0
  ti_currency char(3) encode bytedict,               -- Added in 0.5.0
  ti_price_base dec(18, 2),                          -- Added in 0.5.0
  base_currency char(3) encode bytedict,             -- Added in 0.5.0

  -- Geolocation
  geo_timezone varchar(64) encode text255,           -- Added in 0.5.0

  -- Click ID
  mkt_clickid varchar(128) encode raw,                -- Added in 0.5.0
  mkt_network varchar(64) encode text255,            -- Added in 0.5.0

  -- ETL tags
  etl_tags varchar(500) encode lzo,                  -- Added in 0.5.0

  -- Time event was sent
  dvce_sent_tstamp timestamp,                        -- Added in 0.5.0

  -- Referer
  refr_domain_userid varchar(36),                    -- Added in 0.5.0
  refr_dvce_tstamp timestamp,                        -- Added in 0.5.0

  -- Derived contexts
  derived_contexts varchar(15000) encode lzo,        -- Added in 0.5.0

  -- Session ID
  domain_sessionid char(36) encode raw,              -- Added in 0.5.0

  -- Derived timestamp
  derived_tstamp timestamp,                          -- Added in 0.5.0

  CONSTRAINT event_id_050_pk PRIMARY KEY(event_id)
)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (collector_tstamp);

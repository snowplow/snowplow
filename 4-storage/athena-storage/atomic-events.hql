-- Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
-- Version:     0.9.0
-- URL:         -
--
-- Authors:     Yali Sassoon, Alex Dean, Mike Robins
-- Copyright:   Copyright (c) 2018 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

-- Create the database
CREATE DATABASE IF NOT EXISTS snowplow;

-- Create the events table
CREATE EXTERNAL TABLE IF NOT EXISTS snowplow.events (
    app_id varchar(255),
    platform varchar(255),
    etl_tstamp timestamp,
    collector_tstamp timestamp,
    dvce_created_tstamp timestamp,
    event varchar(128),
    event_id varchar(36),
    txn_id int,
    name_tracker varchar(128),
    v_tracker varchar(100),
    v_collector varchar(100),
    v_etl varchar(100),
    user_id varchar(255),
    user_ipaddress varchar(45),
    user_fingerprint varchar(50),
    domain_userid varchar(36),
    domain_sessionidx int,
    network_userid varchar(38),
    geo_country varchar(2),
    geo_region varchar(2),
    geo_city varchar(75),
    geo_zipcode varchar(15),
    geo_latitude double,
    geo_longitude double,
    geo_region_name varchar(100),
    ip_isp varchar(100),
    ip_organization varchar(100),
    ip_domain varchar(100),
    ip_netspeed varchar(100),
    page_url varchar(4096),
    page_title varchar(2000),
    page_referrer varchar(4096),
    page_urlscheme varchar(16),
    page_urlhost varchar(255),
    page_urlport int,
    page_urlpath varchar(3000),
    page_urlquery varchar(6000),
    page_urlfragment varchar(3000),
    refr_urlscheme varchar(16),
    refr_urlhost varchar(255),
    refr_urlport int,
    refr_urlpath varchar(6000),
    refr_urlquery varchar(6000),
    refr_urlfragment varchar(3000),
    refr_medium varchar(25),
    refr_source varchar(50),
    refr_term varchar(255),
    mkt_medium varchar(255),
    mkt_source varchar(255),
    mkt_term varchar(255),
    mkt_content varchar(500),
    mkt_campaign varchar(255),
    se_category varchar(1000),
    se_action varchar(1000),
    se_label varchar(1000),
    se_property varchar(1000),
    se_value double,
    tr_orderid varchar(255),
    tr_affiliation varchar(255),
    tr_total decimal(18,2),
    tr_tax decimal(18,2),
    tr_shipping decimal(18,2),
    tr_city varchar(255),
    tr_state varchar(255),
    tr_country varchar(255),
    ti_orderid varchar(255),
    ti_sku varchar(255),
    ti_name varchar(255),
    ti_category varchar(255),
    ti_price decimal(18,2),
    ti_quantity int,
    pp_xoffset_min int,
    pp_xoffset_max int,
    pp_yoffset_min int,
    pp_yoffset_max int,
    useragent varchar(1000),
    br_name varchar(50),
    br_family varchar(50),
    br_version varchar(50),
    br_type varchar(50),
    br_renderengine varchar(50),
    br_lang varchar(255),
    br_features_pdf boolean,
    br_features_flash boolean,
    br_features_java boolean,
    br_features_director boolean,
    br_features_quicktime boolean,
    br_features_realplayer boolean,
    br_features_windowsmedia boolean,
    br_features_gears boolean,
    br_features_silverlight boolean,
    br_cookies boolean,
    br_colordepth varchar(12),
    br_viewwidth int,
    br_viewheight int,
    os_name varchar(50),
    os_family varchar(50),
    os_manufacturer varchar(50),
    os_timezone varchar(255),
    dvce_type varchar(50),
    dvce_ismobile boolean,
    dvce_screenwidth int,
    dvce_screenheight int,
    doc_charset varchar(128),
    doc_width int,
    doc_height int,
    tr_currency varchar(3),
    tr_total_base decimal(18, 2),
    tr_tax_base decimal(18, 2),
    tr_shipping_base decimal(18, 2),
    ti_currency varchar(3),
    ti_price_base decimal(18, 2),
    base_currency varchar(3),
    geo_timezone varchar(64),
    mkt_clickid varchar(128),
    mkt_network varchar(64),
    etl_tags varchar(500),
    dvce_sent_tstamp timestamp,
    refr_domain_userid varchar(36),
    refr_dvce_tstamp timestamp,
    domain_sessionid varchar(36),
    derived_tstamp timestamp,
    event_vendor varchar(1000),
    event_name varchar(1000),
    event_format varchar(128),
    event_version varchar(128),
    event_fingerprint varchar(128),
    true_tstamp timestamp
)
PARTITIONED BY (partition TIMESTAMP)
STORED AS PARQUET
LOCATION 's3://{your-bucket}/atomic-events/'
tblproperties ("parquet.compress"="SNAPPY");

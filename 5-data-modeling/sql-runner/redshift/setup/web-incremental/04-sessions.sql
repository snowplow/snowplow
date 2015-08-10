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
-- Authors: Yali Sassoon, Christophe Bogaert
-- Copyright: Copyright (c) 2013-2015 Snowplow Analytics Ltd
-- License: Apache License Version 2.0
--
-- Data Model: web-incremental
-- Version: 2.0
--
-- Table:
-- (a) create the sessions table

CREATE TABLE derived.sessions (

  blended_user_id varchar(255) encode lzo,
  inferred_user_id varchar(255) encode lzo,
  domain_userid varchar(36) encode lzo,
  domain_sessionidx smallint encode lzo,

  session_start_tstamp timestamp encode lzo,
  session_end_tstamp timestamp encode lzo,
  min_dvce_tstamp timestamp encode lzo,
  max_dvce_tstamp timestamp encode lzo,
  max_etl_tstamp timestamp encode lzo,

  event_count	bigint encode lzo,

  time_engaged_with_minutes	double precision encode raw,

  geo_country varchar(255) encode lzo,
  geo_country_code_2_characters char(2) encode lzo,
  geo_country_code_3_characters	char(3) encode lzo,
  geo_region char(2) encode lzo,
  geo_city varchar(75) encode lzo,
  geo_zipcode varchar(15) encode lzo,

  geo_latitude double precision encode raw,
  geo_longitude	double precision encode raw,

  landing_page_host varchar(255) encode lzo,
  landing_page_path varchar(3000) encode lzo,

  exit_page_host varchar(255) encode lzo,
  exit_page_path varchar(3000) encode lzo,

  mkt_source varchar(255) encode lzo,
  mkt_medium varchar(255) encode lzo,
  mkt_term varchar(255) encode lzo,
  mkt_content varchar(500) encode lzo,
  mkt_campaign varchar(255) encode lzo,

  refr_source varchar(50) encode lzo,
  refr_medium varchar(25) encode lzo,
  refr_term varchar(255) encode lzo,
  refr_urlhost varchar(255) encode lzo,
  refr_urlpath varchar(6000) encode lzo,

  br_name varchar(50) encode lzo,
  br_family varchar(50) encode lzo,
  br_version varchar(50) encode lzo,
  br_type varchar(50) encode lzo,
  br_renderengine varchar(50) encode lzo,
  br_lang varchar(255) encode lzo,

  br_features_director boolean encode raw,
  br_features_flash boolean encode raw,
  br_features_gears boolean encode raw,
  br_features_java boolean encode raw,
  br_features_pdf boolean encode raw,
  br_features_quicktime boolean encode raw,
  br_features_realplayer boolean encode raw,
  br_features_silverlight boolean encode raw,
  br_features_windowsmedia boolean encode raw,
  br_cookies boolean encode raw,

  os_name varchar(50) encode lzo,
  os_family varchar(50) encode lzo,
  os_manufacturer varchar(50) encode lzo,
  os_timezone varchar(255) encode lzo,

  dvce_type varchar(50) encode lzo,
  dvce_ismobile boolean encode raw,
  dvce_screenwidth integer encode lzo,
  dvce_screenheight integer encode lzo

)
DISTSTYLE KEY
DISTKEY (domain_userid)
SORTKEY (domain_userid, domain_sessionidx, session_start_tstamp);

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

-- Create the snowplow_pivots.sessions table:
CREATE TABLE IF NOT EXISTS snowplow_pivots.sessions (
  blended_user_id varchar(255) encode runlength,
  inferred_user_id varchar(255) encode runlength,
  domain_userid varchar(36),
  domain_sessionidx smallint,
  session_start_tstamp timestamp,
  session_end_tstamp timestamp,
  dvce_min_tstamp timestamp,
  dvce_max_tstamp timestamp,
  max_etl_tstamp timestamp,
  event_count bigint,
  time_engaged_with_minutes double precision,
  geo_country varchar(255) encode runlength,
  geo_country_code_2_characters char(2) encode runlength,
  geo_country_code_3_characters char(3) encode runlength,
  geo_region char(2) encode runlength,
  geo_city varchar(75) encode runlength,
  geo_zipcode varchar(15) encode runlength,
  geo_latitude double precision encode runlength,
  geo_longitude double precision encode runlength,
  landing_page_host varchar(255) encode text255,
  landing_page_path varchar(3000) encode text32k,
  exit_page_host varchar(255) encode text255,
  exit_page_path varchar(3000) encode text32k,
  mkt_source varchar(255) encode text255,
  mkt_medium varchar(255) encode text255,
  mkt_term varchar(255) encode raw,
  mkt_content varchar(500) encode raw,
  mkt_campaign varchar(255) encode text32k,
  refr_source varchar(50) encode text255,
  refr_medium varchar(25) encode text255,
  refr_term varchar(255) encode raw,
  refr_urlhost varchar(255) encode text255,
  refr_urlpath varchar(3000) encode text32k,
  br_name varchar(50) encode text255,
  br_family varchar(50) encode text255,
  br_version varchar(50) encode text255,
  br_type varchar(50) encode text255,
  br_renderengine varchar(50) encode text255,
  br_lang varchar(255) encode text255,
  br_features_director boolean,
  br_features_flash boolean,
  br_features_gears boolean,
  br_features_java boolean,
  br_features_pdf boolean,
  br_features_quicktime boolean,
  br_features_realplayer boolean, 
  br_features_silverlight boolean,
  br_features_windowsmedia boolean,
  br_cookies boolean,
  os_name varchar(50) encode text255,
  os_family varchar(50) encode text255,
  os_manufacturer varchar(50) encode text255,
  os_timezone varchar(255) encode text255,
  dvce_type varchar(50) encode text255,
  dvce_ismobile boolean,
  dvce_screenwidth integer,
  dvce_screenheight integer
)
DISTSTYLE KEY
DISTKEY (domain_userid)
SORTKEY (domain_userid, domain_sessionidx);

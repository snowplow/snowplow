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
-- (a) create the page_views table

CREATE TABLE derived.page_views (

  blended_user_id varchar(255) encode lzo,
  inferred_user_id varchar(255) encode lzo,
  domain_userid varchar(36) encode lzo,
  domain_sessionidx smallint encode lzo,

  page_urlhost varchar(255) encode lzo,
	page_urlpath varchar(3000) encode lzo,

  first_touch_tstamp timestamp encode lzo,
  last_touch_tstamp timestamp encode lzo,
  min_dvce_tstamp timestamp encode lzo,
  max_dvce_tstamp timestamp encode lzo,
  max_etl_tstamp timestamp encode lzo,

  event_count	bigint encode lzo,
  page_view_count	bigint encode lzo,
  page_ping_count	bigint encode lzo,

  time_engaged_with_minutes	double precision encode raw

)
DISTSTYLE KEY
DISTKEY (domain_userid)
SORTKEY (domain_userid, domain_sessionidx, first_touch_tstamp);

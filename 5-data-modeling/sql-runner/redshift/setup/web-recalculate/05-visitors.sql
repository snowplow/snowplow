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

-- Create the snowplow_pivots.visitors table:
CREATE TABLE IF NOT EXISTS snowplow_pivots.visitors (
  blended_user_id varchar(255) encode runlength,
  first_touch_tstamp timestamp,
  last_touch_tstamp timestamp,
  dvce_min_tstamp timestamp,
  dvce_max_tstamp timestamp,
  max_etl_tstamp timestamp,
  event_count bigint,
  session_count bigint,
  page_view_count bigint,
  time_engaged_with_minutes double precision,

  landing_page_host varchar(255) encode text255,
  landing_page_path varchar(3000) encode text32k,

  mkt_source varchar(255) encode text255,
  mkt_medium varchar(255) encode text255,
  mkt_term varchar(255) encode raw,
  mkt_content varchar(500) encode raw,
  mkt_campaign varchar(255) encode text32k,

  refr_source varchar(50) encode text255,
  refr_medium varchar(25) encode text255,
  refr_term varchar(255) encode raw,
  refr_urlhost varchar(255) encode text255,
  refr_urlpath varchar(3000) encode text32k
)
DISTSTYLE KEY
DISTKEY (blended_user_id)
SORTKEY (blended_user_id);

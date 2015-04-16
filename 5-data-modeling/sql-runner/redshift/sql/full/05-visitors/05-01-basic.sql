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

-- The visitors_basic table has one row per visitor and contains basic information that
-- can be derived from a single table scan. The standard model identifies visitors using only a first party cookie.

DROP TABLE IF EXISTS snowplow_intermediary.visitors_basic;
CREATE TABLE snowplow_intermediary.visitors_basic
  DISTKEY (blended_user_id) -- Optimized to join on other snowplow_intermediary.visitors_X tables
  SORTKEY (blended_user_id) -- Optimized to join on other snowplow_intermediary.visitors_X tables
AS (
  SELECT
    blended_user_id, -- Placeholder (the domain_userid)
    MIN(collector_tstamp) AS first_touch_tstamp,
    MAX(collector_tstamp) AS last_touch_tstamp,
    MIN(dvce_tstamp) AS dvce_min_tstamp, -- Used to replace SQL window functions
    MAX(dvce_tstamp) AS dvce_max_tstamp, -- Used to replace SQL window functions
    MAX(etl_tstamp) AS max_etl_tstamp, -- Used for debugging
    COUNT(*) AS event_count,
    MAX(domain_sessionidx) AS session_count,
    SUM(CASE WHEN event = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
    COUNT(DISTINCT(FLOOR(EXTRACT (EPOCH FROM dvce_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes
  FROM snowplow_intermediary.events_enriched_final
  GROUP BY 1
);

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
-- Page views:
-- (a) aggregate events into page views
-- (b) select all

CREATE TABLE snplw_temp.page_views
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx, first_touch_tstamp)
AS (

WITH basic AS (

  -- (a) aggregate events into page views

  SELECT

    blended_user_id,
    inferred_user_id,
    domain_userid,
    domain_sessionidx,
    page_urlhost,
    page_urlpath,

    MIN(collector_tstamp) AS first_touch_tstamp,
    MAX(collector_tstamp) AS last_touch_tstamp,
    MIN(dvce_created_tstamp) AS min_dvce_created_tstamp, -- used to replace SQL window functions
    MAX(dvce_created_tstamp) AS max_dvce_created_tstamp, -- used to replace SQL window functions
    MAX(etl_tstamp) AS max_etl_tstamp, -- for debugging

    COUNT(*) AS event_count,
    SUM(CASE WHEN event = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
    SUM(CASE WHEN event = 'page_ping' THEN 1 ELSE 0 END) AS page_ping_count,
    COUNT(DISTINCT(FLOOR(EXTRACT (EPOCH FROM dvce_created_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes

  FROM snplw_temp.enriched_events

  WHERE event IN ('page_view','page_ping')
    AND page_urlhost IS NOT NULL -- remove incorrect page views
    AND page_urlpath IS NOT NULL -- remove incorrect page views

  GROUP BY 1,2,3,4,5,6
  ORDER BY 1,2,3,4,7

)

-- (b) select all

SELECT

  b.blended_user_id,
  b.inferred_user_id,
  b.domain_userid,
  b.domain_sessionidx,
  b.page_urlhost,
  b.page_urlpath,

  b.first_touch_tstamp,
  b.last_touch_tstamp,
  b.min_dvce_created_tstamp,
  b.max_dvce_created_tstamp,
  b.max_etl_tstamp,

  b.event_count,
  b.page_view_count,
  b.page_ping_count,
  b.time_engaged_with_minutes

  FROM basic AS b

);

INSERT INTO snplw_temp.queries (SELECT 'page-views', 'new', GETDATE()); -- track time

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
-- Aggregate new and old rows:
-- (a) calculate aggregate frame (i.e. a GROUP BY)
-- (b) calculate initial frame (i.e. first value)
-- (c) combine

CREATE TABLE snplw_temp.visitors_aggregated
  DISTKEY (blended_user_id)
  SORTKEY (blended_user_id)
AS (

WITH aggregate_frame AS (

  -- (a) calculate aggregate frame (i.e. a GROUP BY)

  SELECT

    blended_user_id,

    MIN(first_touch_tstamp) AS first_touch_tstamp,
    MAX(last_touch_tstamp) AS last_touch_tstamp,
    MIN(min_dvce_tstamp) AS min_dvce_tstamp,
    MAX(max_dvce_tstamp) AS max_dvce_tstamp,
    MAX(max_etl_tstamp) AS max_etl_tstamp,
    SUM(event_count) AS event_count,
    MAX(session_count) AS session_count, -- MAX not SUM
    SUM(page_view_count) AS page_view_count,
    SUM(time_engaged_with_minutes) AS time_engaged_with_minutes

  FROM snplw_temp.visitors

  GROUP BY 1
  ORDER BY 1

), initial_frame AS (

  -- (b) calculate initial frame (i.e. first value)

  SELECT * FROM (
    SELECT

      a.blended_user_id,

      a.landing_page_host,
      a.landing_page_path,

      a.mkt_source,
      a.mkt_medium,
      a.mkt_term,
      a.mkt_content,
      a.mkt_campaign,
      a.refr_source,
      a.refr_medium,
      a.refr_term,
      a.refr_urlhost,
      a.refr_urlpath,

      ROW_NUMBER() OVER (PARTITION BY a.blended_user_id) AS row_number

    FROM snplw_temp.visitors AS a

    INNER JOIN aggregate_frame AS b
      ON  a.blended_user_id = b.blended_user_id
      AND a.min_dvce_tstamp = b.min_dvce_tstamp

    ORDER BY 1

  )
  WHERE row_number = 1 -- deduplicate

)

-- (c) combine and insert into derived

SELECT

  a.blended_user_id,

  a.first_touch_tstamp,
  a.last_touch_tstamp,
  a.min_dvce_tstamp,
  a.max_dvce_tstamp,
  a.max_etl_tstamp,
  a.event_count,
  a.session_count,
  a.page_view_count,
  a.time_engaged_with_minutes,

  i.landing_page_host,
  i.landing_page_path,

  i.mkt_source,
  i.mkt_medium,
  i.mkt_term,
  i.mkt_content,
  i.mkt_campaign,

  i.refr_source,
  i.refr_medium,
  i.refr_term,
  i.refr_urlhost,
  i.refr_urlpath

FROM aggregate_frame AS a

LEFT JOIN initial_frame AS i
  ON a.blended_user_id = i.blended_user_id

);

INSERT INTO snplw_temp.queries (SELECT 'visitors', 'aggregate', GETDATE()); -- track time

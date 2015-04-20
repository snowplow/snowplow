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

-- Events belonging to the same visitor can arrive at different times and could end up in different batches.
-- Rows in the visitors_new table therefore have to be merged with those in the pivot table.

-- Select information associated with the first event for each visitor.

DROP TABLE IF EXISTS snowplow_intermediary.visitors_initial_frame;
CREATE TABLE snowplow_intermediary.visitors_initial_frame
  DISTKEY (blended_user_id) -- Optimized to join on other snowplow_intermediary.visitors_X tables
  SORTKEY (blended_user_id) -- Optimized to join on other snowplow_intermediary.visitors_X tables
AS (
  SELECT
    *
  FROM (
    SELECT -- Select the information associated with the earliest event (based on dvce_tstamp)
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
      RANK() OVER (PARTITION BY a.blended_user_id
        ORDER BY a.landing_page_host, a.landing_page_path, a.mkt_source, a.mkt_medium, a.mkt_term, a.mkt_content,
          a.mkt_campaign, a.refr_source, a.refr_medium, a.refr_term, a.refr_urlhost, a.refr_urlpath) AS rank
    FROM snowplow_intermediary.visitors_new AS a
    INNER JOIN snowplow_intermediary.visitors_aggregate_frame AS b
      ON  a.blended_user_id = b.blended_user_id
      AND a.dvce_min_tstamp = b.dvce_min_tstamp -- Replaces the FIRST VALUE window function in SQL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13 -- Aggregate identital rows (that happen to have the same dvce_tstamp)
  )
  WHERE rank = 1 -- If there are different rows with the same dvce_tstamp, rank and pick the first row
);

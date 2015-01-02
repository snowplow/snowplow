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

-- The visitors_landing_page table has one row per visitor and contains the initial landing page.
-- The standard model identifies visitors using only a first party cookie.

DROP TABLE IF EXISTS snowplow_intermediary.visitors_landing_page;
CREATE TABLE snowplow_intermediary.visitors_landing_page
  DISTKEY (blended_user_id) -- Optimized to join on other snowplow_intermediary.visitors_X tables
  SORTKEY (blended_user_id) -- Optimized to join on other snowplow_intermediary.visitors_X tables
AS (
  SELECT
    *
  FROM (
    SELECT -- Select the first page (using dvce_tstamp)
      a.blended_user_id,
      a.page_urlhost,
      a.page_urlpath,
      RANK() OVER (PARTITION BY a.blended_user_id ORDER BY a.page_urlhost, a.page_urlpath) AS rank
    FROM snowplow_intermediary.events_enriched_final AS a
    INNER JOIN snowplow_intermediary.visitors_basic AS b
      ON  a.blended_user_id = b.blended_user_id
      AND a.dvce_tstamp = b.dvce_min_tstamp -- Replaces the FIRST VALUE window function in SQL
    GROUP BY 1,2,3 -- Aggregate identital rows (that happen to have the same dvce_tstamp)
  )
  WHERE rank = 1 -- If there are different rows with the same dvce_tstamp, rank and pick the first row
);

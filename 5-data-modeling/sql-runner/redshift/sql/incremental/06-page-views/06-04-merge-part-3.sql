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

-- Events belonging to the same page view can arrive at different times and could end up in different batches.
-- Rows in the page_views_new table therefore have to be merged with those in the pivot table.

-- Select information associated with the last event in each page view.

DROP TABLE IF EXISTS snowplow_intermediary.page_views_final_frame;
CREATE TABLE snowplow_intermediary.page_views_final_frame
  DISTKEY (domain_userid) -- Optimized to join on other snowplow_intermediary.page_views_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other snowplow_intermediary.page_views_X tables
AS (
  SELECT
    *
  FROM (
    SELECT -- Select the information associated with the earliest event (based on dvce_tstamp)
      a.domain_userid,
      a.domain_sessionidx,
      a.page_urlhost,
      a.page_urlpath,
      a.blended_user_id, -- Edge case: one page view with multiple logins and events in several batches
      a.inferred_user_id, -- Edge case: one page view with multiple logins and events in several batches
      RANK() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx, a.page_urlhost, a.page_urlpath
        ORDER BY a.blended_user_id, a.inferred_user_id) AS rank
    FROM snowplow_intermediary.page_views_new AS a
    INNER JOIN snowplow_intermediary.page_views_aggregate_frame AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.page_urlhost = b.page_urlhost
      AND a.page_urlpath = b.page_urlpath
      AND a.dvce_max_tstamp = b.dvce_max_tstamp -- Replaces the LAST VALUE window function in SQL
    GROUP BY 1,2,3,4,5,6 -- Aggregate identital rows (that happen to have the same dvce_tstamp)
  )
  WHERE rank = 1 -- If there are different rows with the same dvce_tstamp, rank and pick the first row
);

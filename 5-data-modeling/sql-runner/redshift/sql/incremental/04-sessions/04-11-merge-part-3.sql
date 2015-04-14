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

-- The standard model identifies sessions using only first party cookies and session domain indexes,
-- but contains placeholders for identity stitching.

-- Events belonging to the same session can arrive at different times and could end up in different batches.
-- Rows in the sessions_new table therefore have to be merged with those in the pivot table.

-- Select information associated with the last event in each session.

DROP TABLE IF EXISTS snowplow_intermediary.sessions_final_frame;
CREATE TABLE snowplow_intermediary.sessions_final_frame
  DISTKEY (domain_userid) -- Optimized to join on other snowplow_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other snowplow_intermediary.session_X tables
AS (
  SELECT
    *
  FROM (
    SELECT -- Select the information associated with the earliest event (based on dvce_tstamp)
      a.domain_userid,
      a.domain_sessionidx,
      a.blended_user_id, -- Edge case: one session with multiple logins and events in several batches
      a.inferred_user_id, -- Edge case: one session with multiple logins and events in several batches
      a.exit_page_host,
      a.exit_page_path,
      RANK() OVER (PARTITION BY a.domain_userid, a.domain_sessionidx
        ORDER BY a.blended_user_id, a.inferred_user_id, a.exit_page_host, a.exit_page_path) AS rank
    FROM snowplow_intermediary.sessions_new AS a
    INNER JOIN snowplow_intermediary.sessions_aggregate_frame AS b
      ON  a.domain_userid = b.domain_userid
      AND a.domain_sessionidx = b.domain_sessionidx
      AND a.dvce_max_tstamp = b.dvce_max_tstamp -- Replaces the LAST VALUE window function in SQL
    GROUP BY 1,2,3,4,5,6 -- Aggregate identital rows (that happen to have the same dvce_tstamp)
  )
  WHERE rank = 1 -- If there are different rows with the same dvce_tstamp, rank and pick the first row
);

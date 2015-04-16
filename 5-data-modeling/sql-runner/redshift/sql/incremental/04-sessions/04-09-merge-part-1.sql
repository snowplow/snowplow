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

-- First, aggregate timestamps and counts per session.

DROP TABLE IF EXISTS snowplow_intermediary.sessions_aggregate_frame;
CREATE TABLE snowplow_intermediary.sessions_aggregate_frame
  DISTKEY (domain_userid) -- Optimized to join on other snowplow_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other snowplow_intermediary.session_X tables
AS (
  SELECT
    domain_userid,
    domain_sessionidx,
    MIN(session_start_tstamp) AS session_start_tstamp,
    MAX(session_end_tstamp) AS session_end_tstamp,
    MIN(dvce_min_tstamp) AS dvce_min_tstamp,
    MAX(dvce_max_tstamp) AS dvce_max_tstamp,
    MAX(max_etl_tstamp) AS max_etl_tstamp,
    SUM(event_count) AS event_count,
    SUM(time_engaged_with_minutes) AS time_engaged_with_minutes
  FROM snowplow_intermediary.sessions_new
  GROUP BY 1,2
);

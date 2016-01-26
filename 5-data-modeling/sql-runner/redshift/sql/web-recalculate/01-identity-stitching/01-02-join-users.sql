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

-- Enrich events with unstructured events and inferred_user_id. Second, join with the cookie_id_user_id_map.

DROP TABLE IF EXISTS snowplow_intermediary.events_enriched_final;
CREATE TABLE snowplow_intermediary.events_enriched_final
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx, dvce_created_tstamp) -- Sort on dvce_created_tstamp to speed up future queries
AS (
  SELECT
    COALESCE(u.inferred_user_id, e.domain_userid) AS blended_user_id, -- Placeholder (domain_userid)
    u.inferred_user_id, -- Placeholder (NULL)
    e.*
  FROM
    (
    SELECT
      e.*
    FROM
      atomic.events e
    WHERE e.domain_userid IS NOT NULL -- Do not aggregate NULL
      AND e.domain_sessionidx IS NOT NULL -- Do not aggregate NULL
      AND e.domain_userid != '' -- Do not aggregate missing domain_userids
      AND e.dvce_created_tstamp IS NOT NULL -- Required, dvce_created_tstamp is used to sort events
      AND e.collector_tstamp > '2000-01-01' -- Remove incorrect collector_tstamps, can cause SQL errors
      AND e.collector_tstamp < '2030-01-01' -- Remove incorrect collector_tstamps, can cause SQL errors
      AND e.dvce_created_tstamp > '2000-01-01' -- Remove incorrect dvce_created_tstamps, can cause SQL errors
      AND e.dvce_created_tstamp < '2030-01-01' -- Remove incorrect dvce_created_tstamps, can cause SQL errors
    ) e
  LEFT JOIN snowplow_intermediary.cookie_id_to_user_id_map u
    ON u.domain_userid = e.domain_userid
);

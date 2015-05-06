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

-- A simple table that contains one row per structured event. To be merged with the other tables in
-- snowplow_pivots on domain_userid and domain_sessionidx.

INSERT INTO snowplow_pivots.structured_events
(
  SELECT
    blended_user_id,
    inferred_user_id,
    domain_userid,
    domain_sessionidx,
    etl_tstamp, -- For debugging
    dvce_tstamp,
    collector_tstamp,
    se_category,
    se_action,
    se_label,
    se_property,
    se_value
  FROM snowplow_intermediary.events_enriched_final
  WHERE event = 'struct' -- Restrict to structured events
);

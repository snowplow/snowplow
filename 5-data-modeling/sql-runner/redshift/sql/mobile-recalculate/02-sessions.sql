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
-- Authors: Christophe Bogaert
-- Copyright: Copyright (c) 2015 Snowplow Analytics Ltd
-- License: Apache License Version 2.0
--
-- Data Model: mobile-recalculate
-- Version: 0.1
--
-- Mobile sessions:
-- (a) select the first and last events per session
-- (b) compress into single rows

DROP TABLE IF EXISTS derived.mobile_sessions;
CREATE TABLE derived.mobile_sessions
  DISTKEY (mobile_id)
  SORTKEY (mobile_id, mobile_session_idx)
AS (

WITH sessions_1 AS (

  -- (a) select the first and last events per session

  SELECT

    mobile_id,
    mobile_session_idx,

    collector_tstamp,
    dvce_created_tstamp,

    FIRST_VALUE(os_type IGNORE NULLS) OVER (PARTITION BY mobile_id, mobile_session_idx ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS os_type,
    FIRST_VALUE(os_version IGNORE NULLS) OVER (PARTITION BY mobile_id, mobile_session_idx ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS os_version,
    FIRST_VALUE(device_manufacturer IGNORE NULLS) OVER (PARTITION BY mobile_id, mobile_session_idx ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_manufacturer,
    FIRST_VALUE(device_model IGNORE NULLS) OVER (PARTITION BY mobile_id, mobile_session_idx ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_model,
    FIRST_VALUE(carrier IGNORE NULLS) OVER (PARTITION BY mobile_id, mobile_session_idx ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS carrier

  FROM derived.mobile_events

  ORDER BY mobile_id, mobile_session_idx, dvce_created_tstamp

), sessions_2 AS (

  -- (b) compress into single rows

  SELECT

    mobile_id,
    mobile_session_idx,

    MIN(collector_tstamp) AS session_start_tstamp,
    MAX(collector_tstamp) AS session_end_tstamp,
    MIN(dvce_created_tstamp) AS min_dvce_created_tstamp,
    MAX(dvce_created_tstamp) AS max_dvce_created_tstamp,

    os_type,
    os_version,
    device_manufacturer,
    device_model,
    carrier

  FROM sessions_1

  GROUP BY mobile_id, mobile_session_idx, os_type, os_version, device_manufacturer, device_model, carrier
  ORDER BY mobile_id, mobile_session_idx

)

SELECT * FROM sessions_2 ORDER BY mobile_id, mobile_session_idx

);

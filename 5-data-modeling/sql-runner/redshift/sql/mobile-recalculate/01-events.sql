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
-- Mobile events:
-- (a) create a new schema
-- (b) restructure mobile events
-- (c) use LAG to set the previous device timestamp
-- (d) compare the current and previous device timestamp to find when new sessions begin
-- (e) use SUM to create a session index
-- (f) select the relevant columns

CREATE SCHEMA IF NOT EXISTS derived; -- (a) create a new schema

DROP TABLE IF EXISTS derived.mobile_events;
CREATE TABLE derived.mobile_events
  DISTKEY (mobile_id)
  SORTKEY (mobile_id, mobile_session_idx, dvce_created_tstamp)
AS (

WITH events_1 AS (

  -- (b) restructure mobile events

  SELECT

    CASE
      WHEN a.os_type = 'ios' THEN a.apple_idfv
      WHEN a.os_type = 'android' THEN a.android_idfa
      ELSE NULL
    END AS mobile_id, -- alternatives: open_idfa and apple_idfa

    b.collector_tstamp, -- used to compare different users
    b.dvce_created_tstamp, -- used to compare events belong to the same user

    a.os_type,
    a.os_version,
    a.device_manufacturer,
    a.device_model,
    a.carrier

  FROM atomic.com_snowplowanalytics_snowplow_mobile_context_1 AS a

  INNER JOIN atomic.events AS b
    ON a.root_id = b.event_id

  WHERE (a.apple_idfv IS NOT NULL OR a.android_idfa IS NOT NULL) -- optional: add additional restrictions

  ORDER BY mobile_id, dvce_created_tstamp

), events_2 AS (

  -- (c) use LAG to set the previous device timestamp

  SELECT

    *, LAG(dvce_created_tstamp, 1) OVER (PARTITION BY mobile_id ORDER BY dvce_created_tstamp) AS previous_dvce_created_tstamp

  FROM events_1
  ORDER BY mobile_id, dvce_created_tstamp

), events_3 AS (

  -- (d) compare the current and previous device timestamp to find when new sessions begin

  SELECT

    *, CASE WHEN EXTRACT(EPOCH FROM (dvce_created_tstamp - previous_dvce_created_tstamp)) < 60*30 THEN 0 ELSE 1 END AS new_session

  FROM events_2
  ORDER BY mobile_id, dvce_created_tstamp

), events_4 AS (

  -- (e) use SUM to create a session index

  SELECT

    *, SUM(new_session) OVER (PARTITION BY mobile_id ORDER BY dvce_created_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mobile_session_idx

  FROM events_3
  ORDER BY mobile_id, mobile_session_idx, dvce_created_tstamp


)

-- (f) select the relevant columns

SELECT

  mobile_id,
  mobile_session_idx,

  collector_tstamp,
  dvce_created_tstamp,

  os_type,
  os_version,
  device_manufacturer,
  device_model,
  carrier

FROM events_4
ORDER BY mobile_id, mobile_session_idx, dvce_created_tstamp

);

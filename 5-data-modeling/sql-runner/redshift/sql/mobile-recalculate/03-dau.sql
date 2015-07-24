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
-- Mobile DAU:
-- (a) select the first and last events
-- (b) compress into single rows

DROP TABLE IF EXISTS derived.mobile_dau;
CREATE TABLE derived.mobile_dau
  DISTKEY (mobile_id)
  SORTKEY (mobile_id, date)
AS (

WITH dau_1 AS (

  -- (a) select the first and last events

  SELECT

    mobile_id,
    collector_tstamp::date AS date,

    FIRST_VALUE(os_type IGNORE NULLS) OVER (PARTITION BY mobile_id, collector_tstamp::date ORDER BY dvce_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS os_type,
    FIRST_VALUE(os_version IGNORE NULLS) OVER (PARTITION BY mobile_id, collector_tstamp::date ORDER BY dvce_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS os_version,
    FIRST_VALUE(device_manufacturer IGNORE NULLS) OVER (PARTITION BY mobile_id, collector_tstamp::date ORDER BY dvce_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_manufacturer,
    FIRST_VALUE(device_model IGNORE NULLS) OVER (PARTITION BY mobile_id, collector_tstamp::date ORDER BY dvce_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_model,
    FIRST_VALUE(carrier IGNORE NULLS) OVER (PARTITION BY mobile_id, collector_tstamp::date ORDER BY dvce_tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS carrier

  FROM derived.mobile_events

  ORDER BY mobile_id, date

), dau_2 AS (

  -- (b) compress into single rows

  SELECT

    mobile_id,
    date,

    os_type,
    os_version,
    device_manufacturer,
    device_model,
    carrier

  FROM dau_1

  GROUP BY mobile_id, date, os_type, os_version, device_manufacturer, device_model, carrier
  ORDER BY mobile_id, date

)

SELECT * FROM dau_2 ORDER BY mobile_id, date

);

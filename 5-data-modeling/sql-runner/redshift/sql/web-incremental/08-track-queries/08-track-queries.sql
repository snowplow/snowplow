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
--
-- Data Model: web-incremental
-- Version: 2.0
--
-- Track queries:
-- (a) calculate how long each step took to execute

INSERT INTO derived.queries (

  SELECT

    min_tstamp,
    max_tstamp,
    component,
    step,
    tstamp,
    EXTRACT(EPOCH FROM (tstamp - previous_tstamp)) AS duration

  FROM (

    SELECT

      FIRST_VALUE(tstamp) OVER (ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min_tstamp,
      LAST_VALUE(tstamp) OVER (ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_tstamp,

      component,
      step,
      tstamp,

      LAG(tstamp, 1) OVER (ORDER BY tstamp) AS previous_tstamp

    FROM snplw_temp.queries

    ORDER BY tstamp

  )

  WHERE component != 'main'
  ORDER BY tstamp

);

DROP SCHEMA IF EXISTS snplw_temp CASCADE;

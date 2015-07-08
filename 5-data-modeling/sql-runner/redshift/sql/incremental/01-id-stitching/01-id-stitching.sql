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
-- Data Model: Example incremental model
-- Version: 2.0
--
-- Identity stitching:
-- (a) select max device timestamp
-- (b) select the most recent user ID associated with each cookie and deduplicate
-- (c) select the updated rows
-- (d) select the rows that were not updated

INSERT INTO derived.id_map (

WITH stitching_1 AS (

  -- (a) select max device timestamp

  SELECT

    domain_userid,
    MAX(dvce_tstamp) AS max_dvce_tstamp -- the last event where user ID was not NULL

  FROM landing.events

  WHERE user_id IS NOT NULL -- restrict to cookies with a user ID

    AND domain_userid != ''           -- do not aggregate missing values
    AND domain_userid IS NOT NULL     -- do not aggregate NULL
    AND domain_sessionidx IS NOT NULL -- do not aggregate NULL
    AND collector_tstamp IS NOT NULL  -- not required
    AND dvce_tstamp IS NOT NULL       -- not required

    AND dvce_tstamp < DATEADD(year, +1, e.collector_tstamp) -- remove outliers (can cause errors)
    AND dvce_tstamp > DATEADD(year, -1, e.collector_tstamp) -- remove outliers (can cause errors)

  --AND app_id = 'production'
  --AND platform = ''
  --AND page_urlhost = ''
  --AND page_urlpath IS NOT NULL

  GROUP BY 1

), stitching_2 AS (

  -- (b) select the most recent user ID associated with each cookie and deduplicate

  SELECT * FROM (
    SELECT

      a.domain_userid,
      a.user_id,

      ROW_NUMBER() OVER (PARTITION BY a.domain_userid) AS row_number

    FROM landing.events AS a

    INNER JOIN stitching_1 AS b
      ON  a.domain_userid = b.domain_userid
      AND a.dvce_tstamp = b.max_dvce_tstamp -- replaces the LAST VALUE window function in SQL

  ) WHERE row_number = 1 -- deduplicate

), stitching_3 AS (

  -- (c) select the updated rows

  SELECT domain_userid, user_id AS inferred_user_id FROM stitching_2

), stitching_4 AS (

  -- (d) select the rows that were not updated

  SELECT
    o.*
  FROM snplw_temp.id_map AS o
  LEFT JOIN stitching_3 AS n
    ON o.domain_userid = n.domain_userid
  WHERE n.domain_userid IS NULL -- filter out all records where there is a corresponding domain_userid in the new table

)

SELECT * FROM stitching_3
  UNION
SELECT * FROM stitching_4

);

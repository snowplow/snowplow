-- Copyright (c) 2013-2022 Snowplow Analytics Ltd. All rights reserved.
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
-- Copyright: Copyright (c) 2015-2022 Snowplow Analytics Ltd
-- License: Apache License Version 2.0
--
-- Data Model: deduplicate
-- Version: 0.3
--
-- These SQL queries can be used to deduplicate unstructured event or custom context tables. Note that some
-- contexts might have legitimate duplicates (e.g. 2 or more product contexts that join to the same event_id).
-- If that is the case, make sure that the context is defined in such a way that no 2 identical contexts are
-- ever sent with the same event.
--
-- Steps (a) to (c) combine rows that are identical except for root_tstamp (the collector timestamp).
--
-- Step (d) is an optional step that moves all remaining duplicates (same event_id but at least one
-- field other than root_tstamp is different) from atomic to duplicates. Note that this might
-- delete legitimate events from atomic.
--
-- To run these queries replace 'example_unstruct' with the actual table name in atomic. If step (d) is
-- used the same table needs to be created in duplicates.

DROP TABLE IF EXISTS duplicates.tmp_example_unstruct; -- todo: replace placeholder name
DROP TABLE IF EXISTS duplicates.tmp_example_unstruct_id; -- todo: replace placeholder name
DROP TABLE IF EXISTS duplicates.tmp_example_unstruct_id_remaining; -- todo: replace placeholder name

-- (a) list all root_id that occur more than once in the target table

CREATE TABLE duplicates.tmp_example_unstruct_id -- todo: replace placeholder name
  DISTKEY (root_id)
  SORTKEY (root_id)
AS (

  SELECT root_id
  FROM (

    SELECT root_id, COUNT(*) AS count
    FROM atomic.example_unstruct
    --WHERE root_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
    GROUP BY 1

  )

  WHERE count > 1

); -- todo: replace placeholder name

-- (b) create a new table with these events and deduplicate as much as possible using GROUP BY

CREATE TABLE duplicates.tmp_example_unstruct -- todo: replace placeholder name
  DISTKEY (root_id)
  SORTKEY (root_id)
AS (

  SELECT

    schema_vendor,
    schema_name,
    schema_format,
    schema_version,

    root_id,
    MIN(root_tstamp), -- keep the earliest event
    ref_root,
    ref_tree,
    ref_parent,

    -- todo: add all other columns

  FROM atomic.example_unstruct -- todo: replace placeholder name
  WHERE root_id IN (SELECT root_id FROM duplicates.tmp_example_unstruct_id) -- todo: replace placeholder name
    --AND root_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
  GROUP BY 1,2,3,4,5,7,8,9 -- todo: add all remaining columns (except root_tstamp)

);

-- (c) delete the duplicates from the original table and insert the deduplicated rows

BEGIN;

  DELETE FROM atomic.example_unstruct
  WHERE root_id IN (SELECT root_id FROM duplicates.tmp_example_unstruct_id) -- todo: replace placeholder name
    --AND root_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
  ;

  INSERT INTO atomic.example_unstruct (SELECT * FROM duplicates.tmp_example_unstruct); -- todo: replace placeholder name

COMMIT;

-- (d) move remaining duplicates to another table (optional)

--CREATE TABLE duplicates.tmp_example_unstruct_id_remaining -- todo: replace placeholder name
  --DISTKEY (root_id)
  --SORTKEY (root_id)
  --AS (

    --SELECT root_id
    --FROM (

      --SELECT root_id, COUNT(*) AS count
      --FROM atomic.example_unstruct -- todo: replace placeholder name
      --WHERE root_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
      --GROUP BY 1

    --)

    --WHERE count > 1

  --);

--BEGIN;

  --INSERT INTO duplicates.example_unstruct
  --(

    --SELECT * FROM atomic.example_unstruct
    --WHERE root_id IN (SELECT root_id FROM duplicates.tmp_example_unstruct_id_remaining)
    --AND root_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time

  --); -- todo: replace placeholder name

  --DELETE FROM atomic.example_unstruct
  --WHERE root_id IN (SELECT root_id FROM duplicates.tmp_example_unstruct_id_remaining)
    --AND root_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
  --; -- todo: replace placeholder name

--COMMIT;

-- (e) drop tables

DROP TABLE IF EXISTS duplicates.tmp_example_unstruct; -- todo: replace placeholder name
DROP TABLE IF EXISTS duplicates.tmp_example_unstruct_id; -- todo: replace placeholder name
DROP TABLE IF EXISTS duplicates.tmp_example_unstruct_id_remaining; -- todo: replace placeholder name

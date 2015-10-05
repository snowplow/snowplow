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
-- Data Model: deduplicate
-- Version: 0.3

DROP TABLE IF EXISTS duplicates.tmp_example_unstruct; -- todo: replace placeholder name
DROP TABLE IF EXISTS duplicates.tmp_example_unstruct_id; -- todo: replace placeholder name

-- (a) list all root_id that occur more than once in the target table

CREATE TABLE duplicates.tmp_example_unstruct_id -- todo: replace placeholder name
  DISTKEY (root_id)
  SORTKEY (root_id)
AS (SELECT root_id FROM (SELECT root_id, COUNT(*) AS count FROM atomic.com_acme_example_unstruct_1 GROUP BY 1) WHERE count > 1); -- todo: replace placeholder name

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

  FROM atomic.com_acme_example_unstruct_1 -- todo: replace placeholder name
  WHERE root_id IN (SELECT root_id FROM duplicates.tmp_example_unstruct_id) -- todo: replace placeholder name
  GROUP BY 1,2,3,4,5,7,8,9 -- todo: add all remaining columns (except root_tstamp)

);

-- (c) delete the duplicates from the original table and insert the deduplicated rows

BEGIN;

  DELETE FROM atomic.com_acme_example_unstruct_1 WHERE root_id IN (SELECT root_id FROM duplicates.tmp_example_unstruct_id); -- todo: replace placeholder name
  INSERT INTO atomic.com_acme_example_unstruct_1 (SELECT * FROM duplicates.tmp_example_unstruct); -- todo: replace placeholder name

COMMIT;

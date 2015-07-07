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
-- Add old entries that also appear in this new batch:
-- (a) backup selected old visitors
-- (b) move those visitors
-- (c) delete them

BEGIN;

  -- (a) backup selected old visitors
  CREATE TABLE snplw_temp.visitors_backup
    DISTKEY (blended_user_id)
    SORTKEY (blended_user_id)
  AS (SELECT * FROM derived.visitors WHERE blended_user_id IN (SELECT blended_user_id FROM snplw_temp.visitors));

  -- (b) move those visitors
  INSERT INTO snplw_temp.visitors (SELECT * FROM derived.visitors WHERE blended_user_id IN (SELECT blended_user_id FROM snplw_temp.visitors));

  -- (c) delete them
  DELETE FROM derived.visitors WHERE blended_user_id IN (SELECT blended_user_id FROM snplw_temp.visitors);

COMMIT;

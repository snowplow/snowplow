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
-- Preparation:
-- (a) delete the snplw_temp schema
-- (b) recreate the snplow_temp schema
-- (c) back up the user ID map

DROP SCHEMA IF EXISTS snplw_temp CASCADE; -- (a) delete the snplw_temp schema
CREATE SCHEMA snplw_temp; -- (b) recreate the snplow_temp schema

BEGIN; -- (c) back up the user ID map

  CREATE TABLE snplw_temp.id_map
    DISTKEY (domain_userid)
    SORTKEY (domain_userid)
  AS (SELECT * FROM derived.id_map);

  DELETE FROM derived.id_map;

COMMIT;

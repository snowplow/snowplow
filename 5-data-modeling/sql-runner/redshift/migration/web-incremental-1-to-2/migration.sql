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
-- Data Model: web-incremental
-- Version: 2.0
--
-- Migration (assumes setup has been completed):
-- (a) rename columns
-- (b) insert rows into derived
-- (c) delete the old schemas (make sure nothing went wrong before executing this step)
-- (d) note: the storageloader target might also need to be updated

-- (a) rename columns

ALTER TABLE snowplow_pivots.sessions RENAME COLUMN dvce_min_tstamp TO min_dvce_tstamp;
ALTER TABLE snowplow_pivots.sessions RENAME COLUMN dvce_max_tstamp TO max_dvce_tstamp;

ALTER TABLE snowplow_pivots.visitors RENAME COLUMN dvce_min_tstamp TO min_dvce_tstamp;
ALTER TABLE snowplow_pivots.visitors RENAME COLUMN dvce_max_tstamp TO max_dvce_tstamp;

ALTER TABLE snowplow_pivots.page_views RENAME COLUMN dvce_min_tstamp TO min_dvce_tstamp;
ALTER TABLE snowplow_pivots.page_views RENAME COLUMN dvce_max_tstamp TO max_dvce_tstamp;

-- (b) insert rows into derived

INSERT INTO derived.id_map (SELECT * FROM snowplow_intermediary.cookie_id_to_user_id_map);
INSERT INTO derived.sessions (SELECT * FROM snowplow_pivots.sessions);
INSERT INTO derived.visitors (SELECT * FROM snowplow_pivots.visitors);
INSERT INTO derived.page_views (SELECT * FROM snowplow_pivots.page_views);

-- (c) delete the old schemas (make sure nothing went wrong before executing this step)

DROP SCHEMA IF EXISTS snowplow_landing CASCADE;
DROP SCHEMA IF EXISTS snowplow_intermediary CASCADE;
DROP SCHEMA IF EXISTS snowplow_pivots CASCADE;

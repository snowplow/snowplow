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
-- Add old entries that also appear in this new batch:
-- (a) backup selected old sessions
-- (b) move those sessions
-- (c) delete them

BEGIN;

  -- (a) backup selected old sessions
  CREATE TABLE snplw_temp.sessions_backup
    DISTKEY (domain_userid)
    SORTKEY (domain_userid, domain_sessionidx, min_dvce_tstamp)
  AS (SELECT * FROM derived.sessions WHERE domain_userid IN (SELECT domain_userid FROM snplw_temp.sessions));

  -- (b) move those sessions
  INSERT INTO snplw_temp.sessions (SELECT * FROM derived.sessions WHERE domain_userid IN (SELECT domain_userid FROM snplw_temp.sessions));

  -- (c) delete them
  DELETE FROM derived.sessions WHERE domain_userid IN (SELECT domain_userid FROM snplw_temp.sessions);

COMMIT;

INSERT INTO snplw_temp.queries (SELECT 'sessions', 'move-to-new', GETDATE()); -- track time

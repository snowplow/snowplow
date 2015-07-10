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
-- (a) backup selected old page views
-- (b) move those page views
-- (c) delete them

BEGIN;

  -- (a) backup selected old page views
  CREATE TABLE snplw_temp.page_views_backup
    DISTKEY (domain_userid)
    SORTKEY (domain_userid, domain_sessionidx, first_touch_tstamp)
  AS (SELECT * FROM derived.page_views WHERE domain_userid IN (SELECT domain_userid FROM snplw_temp.page_views));

  -- (b) move those page views
  INSERT INTO snplw_temp.page_views (SELECT * FROM derived.page_views WHERE domain_userid IN (SELECT domain_userid FROM snplw_temp.page_views));

  -- (c) delete them
  DELETE FROM derived.page_views WHERE domain_userid IN (SELECT domain_userid FROM snplw_temp.page_views);

COMMIT;

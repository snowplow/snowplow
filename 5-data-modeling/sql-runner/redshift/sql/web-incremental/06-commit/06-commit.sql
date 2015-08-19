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

BEGIN;

  DELETE FROM derived.sessions WHERE domain_userid IN (SELECT domain_userid FROM snplw_temp.sessions);
  DELETE FROM derived.visitors WHERE blended_user_id IN (SELECT blended_user_id FROM snplw_temp.visitors);
  DELETE FROM derived.page_views WHERE domain_userid IN (SELECT domain_userid FROM snplw_temp.page_views);

  INSERT INTO derived.sessions (SELECT * FROM snplw_temp.sessions_aggregated);
  INSERT INTO derived.visitors (SELECT * FROM snplw_temp.visitors_aggregated);
  INSERT INTO derived.page_views (SELECT * FROM snplw_temp.page_views_aggregated);

  INSERT INTO atomic.events (SELECT * FROM landing.events);
  DELETE FROM landing.events;

  INSERT INTO atomic.com_example_context_1 (SELECT * FROM landing.com_example_context_1);
  DELETE FROM landing.com_example_context_1;

  INSERT INTO atomic.com_example_unstructured_event_1 (SELECT * FROM landing.com_example_unstructured_event_1);
  DELETE FROM landing.com_example_unstructured_event_1;

COMMIT;

INSERT INTO snplw_temp.queries (SELECT 'commit', 'commit', GETDATE()); -- track time

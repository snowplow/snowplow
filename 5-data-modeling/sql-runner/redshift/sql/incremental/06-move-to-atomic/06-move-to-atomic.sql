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

DROP SCHEMA IF EXISTS snplw_temp CASCADE;

BEGIN;
  INSERT INTO atomic.events (SELECT * FROM landing.events);
  DELETE FROM landing.events;
COMMIT;

-- add all tables in landing
-- have each step in a separate transaction (begin/commit)

BEGIN;
  INSERT INTO atomic.com_example_context_1 (SELECT * FROM landing.com_example_context_1);
  DELETE FROM landing.com_example_context_1;
COMMIT;

BEGIN;
  INSERT INTO atomic.com_example_unstructured_event_1 (SELECT * FROM landing.com_example_unstructured_event_1);
  DELETE FROM landing.com_example_unstructured_event_1;
COMMIT;

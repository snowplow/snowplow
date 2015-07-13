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
-- Vacuum:
-- (a) vacuum landing schema
-- (b) vacuum and analyze derived schema
-- (c) vacuum and analyze atomic

-- (a) vacuum landing schema

VACUUM landing.events;
VACUUM landing.com_example_context_1; -- add all tables in landing
VACUUM landing.com_example_unstructured_event_1; -- add all tables in landing

-- (b) vacuum and analyze derived schema

VACUUM derived.sessions;
VACUUM derived.visitors;
VACUUM derived.page_views;
VACUUM derived.id_map;

ANALYZE derived.sessions;
ANALYZE derived.visitors;
ANALYZE derived.page_views;
ANALYZE derived.id_map;

-- (c) vacuum and analyze atomic

VACUUM atomic.events;
VACUUM atomic.com_example_context_1; -- add all tables in atomic
VACUUM atomic.com_example_unstructured_event_1; -- add all tables in atomic

ANALYZE atomic.events;
ANALYZE atomic.com_example_context_1; -- add all tables in atomic
ANALYZE atomic.com_example_unstructured_event_1; -- add all tables in atomic

INSERT INTO snplw_temp.queries (SELECT 'vacuum', 'vacuum', GETDATE()); -- track time

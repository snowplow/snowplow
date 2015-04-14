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

-- VACUUM tables that don't get deleted with each batch, and do this at the end so errors in this step
-- don't break the update.

-- Part 4

VACUUM snowplow_pivots.sessions;
ANALYZE snowplow_pivots.sessions;

-- Part 5

VACUUM snowplow_pivots.visitors;
ANALYZE snowplow_pivots.visitors;

-- Part 6

VACUUM snowplow_pivots.page_views;
ANALYZE snowplow_pivots.page_views;

-- Part 7

VACUUM snowplow_pivots.structured_events;
ANALYZE snowplow_pivots.structured_events;

-- Part 9

VACUUM atomic.events;
ANALYZE atomic.events;

VACUUM snowplow_landing.events;
ANALYZE snowplow_landing.events;

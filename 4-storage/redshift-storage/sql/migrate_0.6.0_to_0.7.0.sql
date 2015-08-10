-- Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
-- Version:     Ports version 0.6.0 to version 0.7.0
-- URL:         -
--
-- Authors:     Dani Sola
-- Copyright:   Copyright (c) 2015 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

-- Add new columns in atomic.events
BEGIN;
ALTER TABLE atomic.events ADD COLUMN event_vendor varchar(128) encode text255 DEFAULT NULL;
ALTER TABLE atomic.events ADD COLUMN event_name varchar(128) encode text255 DEFAULT NULL;
ALTER TABLE atomic.events ADD COLUMN event_format varchar(25) encode text255 DEFAULT NULL;
ALTER TABLE atomic.events ADD COLUMN event_version varchar(25) encode text255 DEFAULT NULL;
COMMIT;

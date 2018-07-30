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
-- Version:     Ports version 0.6.0 to version 0.8.0
-- URL:         -
--
-- Authors:     Fred Blundun
-- Copyright:   Copyright (c) 2015 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

BEGIN TRANSACTION;

ALTER TABLE atomic.events
	RENAME COLUMN dvce_tstamp TO dvce_created_tstamp;

ALTER TABLE atomic.events
	ADD COLUMN event_vendor varchar(1000) encode lzo;

ALTER TABLE atomic.events
	ADD COLUMN event_name varchar(1000) encode lzo;

ALTER TABLE atomic.events
	ADD COLUMN event_format varchar(128) encode lzo;

ALTER TABLE atomic.events
	ADD COLUMN event_version varchar(128) encode lzo;

ALTER TABLE atomic.events
	ADD COLUMN event_fingerprint varchar(128) encode lzo;

ALTER TABLE atomic.events
	ADD COLUMN true_tstamp timestamp;

ALTER TABLE atomic.events
	DROP COLUMN unstruct_event CASCADE;

ALTER TABLE atomic.events
	DROP COLUMN contexts;

ALTER TABLE atomic.events
	DROP COLUMN derived_contexts;

COMMENT ON TABLE "atomic"."events" IS '0.8.0';

END TRANSACTION;

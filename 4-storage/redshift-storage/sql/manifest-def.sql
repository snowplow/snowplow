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
-- Version:     0.8.0
-- URL:         -
--
-- Authors:     Alex Dean
-- Copyright:   Copyright (c) 2015 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

-- Create manifest table
CREATE TABLE atomic.manifest (
	-- Timestamp for this run of the pipeline
	etl_tstamp timestamp,
	-- Timestamp for when the load transaction was committed
	commit_tstamp timestamp,
	-- Count of events loaded
	event_count bigint,
	-- How many shredded tables were loaded in this run
	shredded_cardinality int
)
DISTSTYLE ALL
SORTKEY (etl_tstamp);

COMMENT ON TABLE "atomic"."manifest" IS '0.1.0';

-- Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
-- Version:     0.1.0
-- URL:         -
--
-- Authors:     Alex Dean
-- Copyright:   Copyright (c) 2014 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

CREATE TABLE atomic.ev_link_clicks (
	-- Identify the parent event this context belongs to
	event_id varchar(38) encode raw not null unique,
	-- Store the event_name as it might change one day
	event_name varchar(128) encode runlength not null,
	-- Storing timestamp improves join performance
	collector_tstamp timestamp encode raw not null,
	-- Properties of this event
	element_id varchar(256) encode text32k,
	element_classes varchar(2048), -- Holds a JSON array. TODO: should be a child table really
	element_target varchar(256) encode text255,
	target_url varchar(4096) encode text32k not null, 
	-- Constraints
	CONSTRAINT ev_link_clicks_010_pk PRIMARY KEY(event_id)
)
DISTSTYLE KEY
-- Optimized join to atomic.events
DISTKEY (event_id)
SORTKEY (collector_tstamp);
